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

#include "taoskInt.h"
#include "tencrypt.h"
#include "tglobal.h"
#include "tsha.h"

/**
 * View (decrypt and display) encrypted configuration file
 * 
 * This function:
 * 1. Loads encryption keys from the data directory
 * 2. Sets global tsCfgKey for decryption
 * 3. Reads and decrypts the specified config file
 * 4. Displays the decrypted content to stdout
 * 
 * @return 0 on success, error code on failure
 */
int32_t taoskViewEncryptedConfig(void) {
  int32_t code = 0;
  char *decryptedData = NULL;
  int32_t dataLen = 0;

  // Validate inputs
  if (g_args.configFilePath[0] == '\0') {
    fprintf(stderr, "Error: Config file path not specified\n");
    fprintf(stderr, "Usage: taosk --view-config <filepath> -d <dataDir>\n");
    return TSDB_CODE_INVALID_PARA;
  }

  // Check if config file exists
  if (!taosCheckExistFile(g_args.configFilePath)) {
    fprintf(stderr, "Error: Config file not found: %s\n", g_args.configFilePath);
    return TSDB_CODE_NOT_FOUND;
  }

  // Check if file is encrypted
  bool isEncrypted = taosIsEncryptedFile(g_args.configFilePath, NULL);
  if (!isEncrypted) {
    fprintf(stderr, "Warning: File is not encrypted, displaying as plain text\n\n");
    
    // Read plain text file
    TdFilePtr pFile = taosOpenFile(g_args.configFilePath, TD_FILE_READ);
    if (pFile == NULL) {
      fprintf(stderr, "Error: Failed to open file: %s\n", tstrerror(terrno));
      return terrno;
    }

    int64_t fileSize = 0;
    code = taosFStatFile(pFile, &fileSize, NULL);
    if (code != 0) {
      taosCloseFile(&pFile);
      return code;
    }

    char *content = taosMemoryMalloc(fileSize + 1);
    if (content == NULL) {
      taosCloseFile(&pFile);
      return TSDB_CODE_OUT_OF_MEMORY;
    }

    if (taosReadFile(pFile, content, fileSize) != fileSize) {
      taosMemoryFree(content);
      taosCloseFile(&pFile);
      return terrno;
    }

    content[fileSize] = '\0';
    printf("%s\n", content);
    taosMemoryFree(content);
    taosCloseFile(&pFile);
    return 0;
  }

  printf("File is encrypted, loading keys...\n\n");

  // Build key file paths (use data directory from global args, already determined in main)
  char masterKeyFile[PATH_MAX] = {0};
  char derivedKeyFile[PATH_MAX] = {0};
  
  snprintf(masterKeyFile, sizeof(masterKeyFile), "%s/dnode/config/%s", 
           g_args.dataDir, MASTER_KEY_FILE_NAME);
  snprintf(derivedKeyFile, sizeof(derivedKeyFile), "%s/dnode/config/%s", 
           g_args.dataDir, DERIVED_KEY_FILE_NAME);

  // Load encryption keys
  char svrKey[ENCRYPT_KEY_LEN + 1] = {0};
  char dbKey[ENCRYPT_KEY_LEN + 1] = {0};
  char cfgKey[ENCRYPT_KEY_LEN + 1] = {0};
  char metaKey[ENCRYPT_KEY_LEN + 1] = {0};
  char dataKey[ENCRYPT_KEY_LEN + 1] = {0};
  int32_t algorithm = 0;
  int32_t cfgAlgorithm = 0;
  int32_t metaAlgorithm = 0;
  int32_t fileVersion = 0;
  int32_t keyVersion = 0;
  int64_t createTime = 0;
  int64_t svrKeyUpdateTime = 0;
  int64_t dbKeyUpdateTime = 0;

  code = taoskLoadEncryptKeys(masterKeyFile, derivedKeyFile, 
                               svrKey, dbKey, cfgKey, metaKey, dataKey,
                               &algorithm, &cfgAlgorithm, &metaAlgorithm,
                               &fileVersion, &keyVersion, &createTime,
                               &svrKeyUpdateTime, &dbKeyUpdateTime);
  if (code != 0) {
    fprintf(stderr, "Error: Failed to load encryption keys: %s\n", tstrerror(code));
    fprintf(stderr, "Please ensure keys are generated in data directory: %s\n", g_args.dataDir);
    fprintf(stderr, "  Master key file: %s\n", masterKeyFile);
    fprintf(stderr, "  Derived key file: %s\n", derivedKeyFile);
    return code;
  }

  // Check if CFG_KEY is available
  if (cfgKey[0] == '\0') {
    fprintf(stderr, "Error: CFG_KEY not found\n");
    fprintf(stderr, "Config file encryption requires CFG_KEY\n");
    fprintf(stderr, "Please generate keys with --encrypt-config option\n");
    code = TSDB_CODE_FAILED;
    goto _exit;
  }

  // Copy CFG_KEY to global variable for taosReadCfgFile
  strncpy(tsCfgKey, cfgKey, ENCRYPT_KEY_LEN);
  tsCfgKey[ENCRYPT_KEY_LEN] = '\0';

  // Set encryption key status to LOADED
  atomic_store_32(&tsEncryptKeysStatus, TSDB_ENCRYPT_KEY_STAT_LOADED);

  printf("CFG_KEY loaded successfully\n");

  // Read and decrypt config file using taosReadCfgFile
  printf("Decrypting config file...\n\n");
  code = taosReadCfgFile(g_args.configFilePath, &decryptedData, &dataLen);
  if (code != 0) {
    fprintf(stderr, "Error: Failed to decrypt config file: %s\n", tstrerror(code));
    goto _exit;
  }

  if (decryptedData == NULL || dataLen == 0) {
    fprintf(stderr, "Error: Decrypted data is empty\n");
    code = TSDB_CODE_FAILED;
    goto _exit;
  }

  // Display decrypted content
  printf("================== Decrypted Config File ==================\n");
  printf("File: %s\n", g_args.configFilePath);
  printf("Size: %d bytes\n", dataLen);
  printf("===========================================================\n\n");
  
  // Ensure null termination
  if (decryptedData[dataLen - 1] != '\0') {
    char *newBuf = taosMemoryRealloc(decryptedData, dataLen + 1);
    if (newBuf != NULL) {
      decryptedData = newBuf;
      decryptedData[dataLen] = '\0';
    }
  }
  
  printf("%s\n", decryptedData);
  printf("\n===========================================================\n");

_exit:
  if (decryptedData != NULL) {
    taosMemoryFree(decryptedData);
  }
  
  // Clear CFG_KEY from global variable for security
  memset(tsCfgKey, 0, sizeof(tsCfgKey));
  
  return code;
}

/**
 * Edit encrypted configuration file with system editor
 *
 * This function:
 * 1. Loads CFG_KEY from data directory
 * 2. Decrypts file to temporary file with 0600 permissions
 * 3. Invokes $EDITOR or vi to edit the file
 * 4. Detects changes via SHA256 hash comparison
 * 5. Re-encrypts and writes back if changed
 * 6. Cleans up temporary file
 *
 * @return 0 on success, error code on failure
 */
 int32_t taoskEditEncryptedFile(void) {
  int32_t code = 0;
  char *decryptedData = NULL;
  int32_t dataLen = 0;
  char *editedData = NULL;
  int32_t editedLen = 0;
  char tempFile[PATH_MAX] = {0};
  uint8_t originalHash[SHA256_DIGEST_SIZE] = {0};
  uint8_t editedHash[SHA256_DIGEST_SIZE] = {0};

  // Validate inputs
  if (g_args.editFilePath[0] == '\0') {
    fprintf(stderr, "Error: File path not specified\n");
    fprintf(stderr, "Usage: taosk --edit-file <filepath> -d <dataDir>\n");
    return TSDB_CODE_INVALID_PARA;
  }

  // Check if file exists
  if (!taosCheckExistFile(g_args.editFilePath)) {
    fprintf(stderr, "Error: File not found: %s\n", g_args.editFilePath);
    return TSDB_CODE_NOT_FOUND;
  }

  // Build key file paths
  char masterKeyFile[PATH_MAX] = {0};
  char derivedKeyFile[PATH_MAX] = {0};

  snprintf(masterKeyFile, sizeof(masterKeyFile), "%s/dnode/config/%s",
           g_args.dataDir, MASTER_KEY_FILE_NAME);
  snprintf(derivedKeyFile, sizeof(derivedKeyFile), "%s/dnode/config/%s",
           g_args.dataDir, DERIVED_KEY_FILE_NAME);

  // Load encryption keys
  char svrKey[ENCRYPT_KEY_LEN + 1] = {0};
  char dbKey[ENCRYPT_KEY_LEN + 1] = {0};
  char cfgKey[ENCRYPT_KEY_LEN + 1] = {0};
  char metaKey[ENCRYPT_KEY_LEN + 1] = {0};
  char dataKey[ENCRYPT_KEY_LEN + 1] = {0};
  int32_t algorithm = 0;
  int32_t cfgAlgorithm = 0;
  int32_t metaAlgorithm = 0;
  int32_t fileVersion = 0;
  int32_t keyVersion = 0;
  int64_t createTime = 0;
  int64_t svrKeyUpdateTime = 0;
  int64_t dbKeyUpdateTime = 0;

  printf("Loading encryption keys from: %s\n", g_args.dataDir);
  code = taoskLoadEncryptKeys(masterKeyFile, derivedKeyFile,
                               svrKey, dbKey, cfgKey, metaKey, dataKey,
                               &algorithm, &cfgAlgorithm, &metaAlgorithm,
                               &fileVersion, &keyVersion, &createTime,
                               &svrKeyUpdateTime, &dbKeyUpdateTime);
  if (code != 0) {
    fprintf(stderr, "Error: Failed to load encryption keys: %s\n", tstrerror(code));
    fprintf(stderr, "Please ensure keys are generated in data directory: %s\n", g_args.dataDir);
    return code;
  }

  // Check if CFG_KEY is available
  if (cfgKey[0] == '\0') {
    fprintf(stderr, "Error: CFG_KEY not found\n");
    fprintf(stderr, "Config file encryption requires CFG_KEY\n");
    fprintf(stderr, "Please generate keys with --encrypt-config option\n");
    return TSDB_CODE_FAILED;
  }

  // Copy CFG_KEY to global variable for taosReadCfgFile/taosWriteCfgFile
  strncpy(tsCfgKey, cfgKey, ENCRYPT_KEY_LEN);
  tsCfgKey[ENCRYPT_KEY_LEN] = '\0';

  // Set encryption key status to LOADED
  atomic_store_32(&tsEncryptKeysStatus, TSDB_ENCRYPT_KEY_STAT_LOADED);

  printf("CFG_KEY loaded successfully\n");

  // Read and decrypt file
  printf("Decrypting file: %s\n", g_args.editFilePath);
  code = taosReadCfgFile(g_args.editFilePath, &decryptedData, &dataLen);
  if (code != 0) {
    fprintf(stderr, "Error: Failed to read/decrypt file: %s\n", tstrerror(code));
    goto _exit;
  }

  if (decryptedData == NULL || dataLen == 0) {
    fprintf(stderr, "Error: File is empty\n");
    code = TSDB_CODE_FAILED;
    goto _exit;
  }

  printf("File decrypted successfully (%d bytes)\n", dataLen);

  // Calculate original content hash for change detection
  sha256((const uint8_t *)decryptedData, dataLen, originalHash);

  // Create temporary file with secure permissions using cross-platform API
  taosGetTmpfilePath(TD_TMP_DIR_PATH, "taosk_edit", tempFile);

  TdFilePtr pTempFile = taosOpenFile(tempFile, TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC | TD_FILE_EXCL);
  if (pTempFile == NULL) {
    fprintf(stderr, "Error: Failed to create temporary file: %s\n", tstrerror(terrno));
    code = terrno;
    goto _exit;
  }

  // Write decrypted content to temp file
  int64_t written = taosWriteFile(pTempFile, decryptedData, dataLen);
  if (written != dataLen) {
    fprintf(stderr, "Error: Failed to write to temporary file: %s\n", tstrerror(terrno));
    taosCloseFile(&pTempFile);
    taosRemoveFile(tempFile);
    tempFile[0] = '\0';
    code = (terrno != 0) ? terrno : TSDB_CODE_FAILED;
    goto _exit;
  }

  // Close temp file
  taosCloseFile(&pTempFile);
  printf("Temporary file created: %s\n", tempFile);

  // Get editor from environment or use default
  const char *editor = getenv("EDITOR");
  if (editor == NULL || editor[0] == '\0') {
    editor = "vi";
  }

  // Build editor command
  char command[PATH_MAX * 2];
  snprintf(command, sizeof(command), "%s \"%s\"", editor, tempFile);

  // Invoke editor
  printf("\nOpening editor: %s\n", editor);
  printf("Edit the file and save to apply changes.\n");
  printf("Exit without saving to cancel.\n\n");

  int ret = system(command);
  if (ret != 0) {
    fprintf(stderr, "Warning: Editor exited with code %d\n", ret);
    // Continue anyway - user may have saved despite non-zero exit
  }

  // Read edited content from temp file
  TdFilePtr pFile = taosOpenFile(tempFile, TD_FILE_READ);
  if (pFile == NULL) {
    fprintf(stderr, "Error: Failed to open temporary file after editing\n");
    code = TSDB_CODE_FAILED;
    goto _exit;
  }

  int64_t fileSize = 0;
  code = taosFStatFile(pFile, &fileSize, NULL);
  if (code != 0) {
    taosCloseFile(&pFile);
    goto _exit;
  }

  editedData = taosMemoryMalloc(fileSize + 1);
  if (editedData == NULL) {
    taosCloseFile(&pFile);
    code = TSDB_CODE_OUT_OF_MEMORY;
    goto _exit;
  }

  if (taosReadFile(pFile, editedData, fileSize) != fileSize) {
    taosCloseFile(&pFile);
    code = terrno;
    goto _exit;
  }

  editedData[fileSize] = '\0';
  editedLen = fileSize;
  taosCloseFile(&pFile);

  // Calculate edited content hash
  sha256((const uint8_t *)editedData, editedLen, editedHash);

  // Compare hashes to detect changes
  if (memcmp(originalHash, editedHash, SHA256_DIGEST_SIZE) == 0) {
    printf("\nNo changes detected, skipping write.\n");
    code = 0;
    goto _exit;
  }

  printf("\nChanges detected, encrypting and writing back...\n");

  // Write encrypted file back
  code = taosWriteCfgFile(g_args.editFilePath, editedData, editedLen);
  if (code != 0) {
    fprintf(stderr, "Error: Failed to write encrypted file: %s\n", tstrerror(code));
    fprintf(stderr, "Temporary file preserved at: %s\n", tempFile);
    fprintf(stderr, "You can manually recover your changes from this file.\n");
    // Don't delete temp file on write failure
    tempFile[0] = '\0';
    goto _exit;
  }

  printf("File updated successfully: %s\n", g_args.editFilePath);
  code = 0;

_exit:
  // Clean up temporary file
  if (tempFile[0] != '\0') {
    taosRemoveFile(tempFile);
  }

  // Clear CFG_KEY from global variable for security
  memset(tsCfgKey, 0, sizeof(tsCfgKey));
  memset(cfgKey, 0, sizeof(cfgKey));

  // Free allocated memory
  if (decryptedData != NULL) {
    taosMemoryFree(decryptedData);
  }
  if (editedData != NULL) {
    taosMemoryFree(editedData);
  }

  return code;
}