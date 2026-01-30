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
#include "tglobal.h"
#include "tencrypt.h"

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

  // Build key file paths
  char masterKeyFile[PATH_MAX] = {0};
  char derivedKeyFile[PATH_MAX] = {0};
  const char *dataDir = g_args.dataDir[0] ? g_args.dataDir : tsDataDir;
  
  snprintf(masterKeyFile, sizeof(masterKeyFile), "%s/dnode/config/%s", 
           dataDir, MASTER_KEY_FILE_NAME);
  snprintf(derivedKeyFile, sizeof(derivedKeyFile), "%s/dnode/config/%s", 
           dataDir, DERIVED_KEY_FILE_NAME);

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
    fprintf(stderr, "Please ensure keys are generated in data directory: %s\n", dataDir);
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
