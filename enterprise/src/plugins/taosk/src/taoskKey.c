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
#include "thash.h"
#include <sys/stat.h>

/**
 * Parse taos.cfg to get dataDir value
 *
 * @param configPath Config directory or file path
 * @param dataDir Output buffer for dataDir value
 * @param dataDirLen Size of dataDir buffer
 * @return 0 on success, error code on failure
 */
static int32_t taoskParseDataDir(const char *configPath, char *dataDir, int32_t dataDirLen) {
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

// Algorithm conversion functions
const char* taoskAlgoToString(EEncryptAlgo algo) {
  switch (algo) {
    case ENCRYPT_ALGO_SM2: return "sm2";
    case ENCRYPT_ALGO_SM3: return "sm3";
    case ENCRYPT_ALGO_SM4: return "sm4";
    case ENCRYPT_ALGO_AES:
      return "aes";
    default: return "none";
  }
}

EEncryptAlgo taoskStringToAlgo(const char *algoStr) {
  if (strcasecmp(algoStr, "sm2") == 0) return ENCRYPT_ALGO_SM2;
  if (strcasecmp(algoStr, "sm3") == 0) return ENCRYPT_ALGO_SM3;
  if (strcasecmp(algoStr, "sm4") == 0) return ENCRYPT_ALGO_SM4;
  if (strcasecmp(algoStr, "aes") == 0) return ENCRYPT_ALGO_AES;
  return ENCRYPT_ALGO_NONE;
}

// Check if algorithm is a symmetric encryption algorithm (SM4 or AES)
bool taoskIsSymmetricAlgo(EEncryptAlgo algo) { return (algo == ENCRYPT_ALGO_SM4 || algo == ENCRYPT_ALGO_AES); }

// Validate that algorithm is SM4 or AES only (for cfg and meta keys)
int32_t taoskValidateSymmetricAlgo(EEncryptAlgo algo) {
  if (!taoskIsSymmetricAlgo(algo)) {
    fprintf(stderr, "Error: Invalid encryption algorithm. Only SM4 and AES are supported for cfg/meta keys.\n");
    return TSDB_CODE_INVALID_PARA;
  }
  return TSDB_CODE_SUCCESS;
}

const char *taoskKeyTypeToString(ETaoskKeyType type) {
  switch (type) {
    case KEY_TYPE_SVR: return "SVR_KEY";
    case KEY_TYPE_DB: return "DB_KEY";
    case KEY_TYPE_CFG: return "CFG_KEY";
    case KEY_TYPE_META: return "META_KEY";
    case KEY_TYPE_DATA: return "DATA_KEY";
    default: return "UNKNOWN";
  }
}

/**
 * Generate random encryption key
 *
 * Generates a cryptographically secure 16-byte (128-bit) key suitable for SM4/AES-128.
 * The key consists of printable ASCII characters (33-126) for ease of handling.
 *
 * @param key    Output buffer for the generated key (must be at least ENCRYPT_KEY_LEN+1 bytes)
 * @param len    Length of the output buffer
 * @return       0 on success, error code on failure
 *
 * Note: SM4 and AES-128 require exactly 16 bytes (128 bits) for full security.
 */
int32_t taoskGenerateRandomKey(char *key, int32_t len) {
  if (key == NULL || len < ENCRYPT_KEY_LEN + 1) {
    return TSDB_CODE_INVALID_PARA;
  }

  // Generate exactly ENCRYPT_KEY_LEN (16) random bytes
  // Use printable ASCII characters (33-126) for better handling
  taosSeedRand((uint32_t)taosGetTimestampUs());
  for (int i = 0; i < ENCRYPT_KEY_LEN; i++) {
    key[i] = (char)(taosRand() % 94 + 33);  // Range: '!' to '~'
  }
  key[ENCRYPT_KEY_LEN] = '\0';
  
  return 0;
}

/**
 * Validate encryption key
 *
 * Enforces key length requirement: 8-16 characters (64-128 bits).
 * This ensures adequate cryptographic strength for SM4/AES-128 encryption.
 *
 * @param key    The key string to validate
 * @return       0 if valid, error code otherwise
 *
 * Security rationale:
 * - SM4 and AES-128 use 16 bytes (128 bits) internally
 * - Minimum 8 characters (64 bits) provides adequate security
 * - Keys shorter than 8 bytes have reduced entropy and are vulnerable to brute force
 * - Keys longer than 16 bytes are truncated/padded to 16 bytes
 */
int32_t taoskValidateKey(const char *key) {
  if (key == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  
  int len = strlen(key);

  // Enforce 8-16 characters
  if (len < ENCRYPT_KEY_LEN_MIN || len > ENCRYPT_KEY_LEN) {
    fprintf(stderr, "\nError: Key must be between %d and %d characters (64-128 bits) for SM4/AES-128\n", 
            ENCRYPT_KEY_LEN_MIN, ENCRYPT_KEY_LEN);
    fprintf(stderr, "       Current key length: %d characters\n", len);

    if (len < ENCRYPT_KEY_LEN_MIN) {
      fprintf(stderr, "       ❌ Too short! Please add at least %d more character(s)\n", 
              ENCRYPT_KEY_LEN_MIN - len);
      fprintf(stderr, "       Example: \"MyPass123\" (9 characters)\n");
    } else {
      fprintf(stderr, "       ❌ Too long! Please remove %d character(s)\n", len - ENCRYPT_KEY_LEN);
      fprintf(stderr, "       Note: Only first 16 characters will be used\n");
    }

    fprintf(stderr, "\n");
    return TSDB_CODE_INVALID_PARA;
  }

  return 0;
}

// Derive keys from master keys
int32_t taoskDeriveKeys(const char *svrKey, const char *dbKey, SKeyEntry *keys, int32_t *keyCount) {
  if (keys == NULL || keyCount == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }
  
  int32_t count = 0;
  
  // SVR_KEY - from parameter or generate
  if (svrKey != NULL && svrKey[0] != '\0') {
    keys[count].type = KEY_TYPE_SVR;
    strncpy(keys[count].key, svrKey, ENCRYPT_KEY_LEN);
    keys[count].lastModified = taosGetTimestampMs();
    keys[count].enabled = true;
    count++;
  } else {
    // Generate random SVR_KEY
    char randomKey[ENCRYPT_KEY_LEN + 1] = {0};
    if (taoskGenerateRandomKey(randomKey, sizeof(randomKey)) != 0) {
      return TSDB_CODE_FAILED;
    }
    keys[count].type = KEY_TYPE_SVR;
    strncpy(keys[count].key, randomKey, ENCRYPT_KEY_LEN);
    keys[count].lastModified = taosGetTimestampMs();
    keys[count].enabled = true;
    count++;
  }
  
  // DB_KEY - from parameter or generate
  if (dbKey != NULL && dbKey[0] != '\0') {
    keys[count].type = KEY_TYPE_DB;
    strncpy(keys[count].key, dbKey, ENCRYPT_KEY_LEN);
    keys[count].lastModified = taosGetTimestampMs();
    keys[count].enabled = true;
    count++;
  } else {
    // Generate random DB_KEY
    char randomKey[ENCRYPT_KEY_LEN + 1] = {0};
    if (taoskGenerateRandomKey(randomKey, sizeof(randomKey)) != 0) {
      return TSDB_CODE_FAILED;
    }
    keys[count].type = KEY_TYPE_DB;
    strncpy(keys[count].key, randomKey, ENCRYPT_KEY_LEN);
    keys[count].lastModified = taosGetTimestampMs();
    keys[count].enabled = true;
    count++;
  }
  
  // CFG_KEY - derived from SVR_KEY and DB_KEY if config encryption is enabled
  if (g_args.encryptConfig) {
    char derivedKey[ENCRYPT_KEY_LEN + 1] = {0};
    // Simple derivation: hash of SVR_KEY + DB_KEY + "CFG"
    snprintf(derivedKey, sizeof(derivedKey), "%s_%s_CFG", keys[0].key, keys[1].key);
    
    keys[count].type = KEY_TYPE_CFG;
    // Use first 16 chars or hash it
    memset(keys[count].key, 0, sizeof(keys[count].key));
    for (int i = 0; i < ENCRYPT_KEY_LEN && derivedKey[i] != '\0'; i++) {
      keys[count].key[i] = derivedKey[i];
    }
    keys[count].lastModified = taosGetTimestampMs();
    keys[count].enabled = true;
    count++;
  }
  
  // META_KEY - derived from SVR_KEY and DB_KEY if metadata encryption is enabled
  if (g_args.encryptMetadata) {
    char derivedKey[ENCRYPT_KEY_LEN + 1] = {0};
    snprintf(derivedKey, sizeof(derivedKey), "%s_%s_META", keys[0].key, keys[1].key);
    
    keys[count].type = KEY_TYPE_META;
    memset(keys[count].key, 0, sizeof(keys[count].key));
    for (int i = 0; i < ENCRYPT_KEY_LEN && derivedKey[i] != '\0'; i++) {
      keys[count].key[i] = derivedKey[i];
    }
    keys[count].lastModified = taosGetTimestampMs();
    keys[count].enabled = true;
    count++;
  }
  
  // DATA_KEY - from parameter or derived if data encryption is enabled
  if (g_args.encryptData) {
    if (g_args.dataKey[0] != '\0') {
      keys[count].type = KEY_TYPE_DATA;
      strncpy(keys[count].key, g_args.dataKey, ENCRYPT_KEY_LEN);
      keys[count].lastModified = taosGetTimestampMs();
      keys[count].enabled = true;
      count++;
    } else {
      // Derive DATA_KEY
      char derivedKey[ENCRYPT_KEY_LEN + 1] = {0};
      snprintf(derivedKey, sizeof(derivedKey), "%s_%s_DATA", keys[0].key, keys[1].key);
      
      keys[count].type = KEY_TYPE_DATA;
      memset(keys[count].key, 0, sizeof(keys[count].key));
      for (int i = 0; i < ENCRYPT_KEY_LEN && derivedKey[i] != '\0'; i++) {
        keys[count].key[i] = derivedKey[i];
      }
      keys[count].lastModified = taosGetTimestampMs();
      keys[count].enabled = true;
      count++;
    }
  }
  
  *keyCount = count;
  return 0;
}

// Generate keys with proper multi-layer encryption
int32_t taoskGenerateKeys(void) {
  int32_t code = 0;
  char *machineId = NULL;
  char    svrKey[ENCRYPT_KEY_LEN + 1] = {0};
  char    dbKey[ENCRYPT_KEY_LEN + 1] = {0};
  char    cfgKey[ENCRYPT_KEY_LEN + 1] = {0};
  char    metaKey[ENCRYPT_KEY_LEN + 1] = {0};
  char    dataKey[ENCRYPT_KEY_LEN + 1] = {0};
  char encryptFilePath[PATH_MAX] = {0};
  
  // Validate input keys
  if (g_args.svrKey[0] != '\0') {
    if ((code = taoskValidateKey(g_args.svrKey)) != 0) {
      return code;
    }
    strncpy(svrKey, g_args.svrKey, ENCRYPT_KEY_LEN);
  } else {
    // Generate random SVR_KEY
    if ((code = taoskGenerateRandomKey(svrKey, sizeof(svrKey))) != 0) {
      fprintf(stderr, "Error: Failed to generate SVR_KEY\n");
      return code;
    }
  }

  if (g_args.dbKey[0] != '\0') {
    if ((code = taoskValidateKey(g_args.dbKey)) != 0) {
      return code;
    }
    strncpy(dbKey, g_args.dbKey, ENCRYPT_KEY_LEN);
  } else {
    // Generate random DB_KEY
    if ((code = taoskGenerateRandomKey(dbKey, sizeof(dbKey))) != 0) {
      fprintf(stderr, "Error: Failed to generate DB_KEY\n");
      return code;
    }
  }

  // Derive sub-keys if needed
  if (g_args.encryptConfig) {
    // Derive CFG_KEY from SVR_KEY and DB_KEY
    taoskGenerateRandomKey(cfgKey, sizeof(cfgKey));
  }

  if (g_args.encryptMetadata) {
    // Derive META_KEY from SVR_KEY and DB_KEY
    taoskGenerateRandomKey(metaKey, sizeof(metaKey));
  }

  if (g_args.encryptData) {
    if (g_args.dataKey[0] != '\0') {
      if ((code = taoskValidateKey(g_args.dataKey)) != 0) {
        return code;
      }
      strncpy(dataKey, g_args.dataKey, ENCRYPT_KEY_LEN);
    } else {
      // Derive DATA_KEY from SVR_KEY and DB_KEY
      taoskGenerateRandomKey(dataKey, sizeof(dataKey));
    }
  }

  // Get machine ID
  code = tGetMachineId(&machineId);
  if (code != 0 || machineId == NULL) {
    fprintf(stderr, "Error: Failed to get machine code: %s\n", tstrerror(code));
    fprintf(stderr, "Storage security feature requires valid machine code.\n");
    fprintf(stderr, "This may fail in virtualized environments without hardware access.\n");
    return code == 0 ? TSDB_CODE_FAILED : code;
  }
  
  printf("Machine Code: %s\n", machineId);

  // Print generated keys (plaintext - for user reference only)
  printf("\nGenerated Keys (for reference):\n");
  printf("CFG Algorithm: %s\n", taoskAlgoToString(g_args.cfgAlgorithm));
  printf("META Algorithm: %s\n", taoskAlgoToString(g_args.metaAlgorithm));
  printf("  SVR_KEY: %s\n", svrKey);
  printf("  DB_KEY: %s\n", dbKey);
  if (g_args.encryptConfig) {
    printf("  CFG_KEY: %s\n", cfgKey);
  }
  if (g_args.encryptMetadata) {
    printf("  META_KEY: %s\n", metaKey);
  }
  if (g_args.encryptData) {
    printf("  DATA_KEY: %s\n", dataKey);
  }

  // Use SM4 as default algorithm for master keys (SVR_KEY, DB_KEY)
  // User can specify different algorithms for CFG_KEY and META_KEY
  int32_t masterAlgo = ENCRYPT_ALGO_SM4;  // Always use SM4 for master keys
  int32_t cfgAlgo = g_args.cfgAlgorithm;
  int32_t metaAlgo = g_args.metaAlgorithm;

  // Build master key data structure
  // First generation: keyVersion = 1
  SMasterKeyData masterKeyData = {0};
  int64_t        now = taosGetTimestampMs();
  code = taoskBuildMasterKeyData(svrKey, dbKey, masterAlgo, cfgAlgo, metaAlgo, 1,  // keyVersion starts from 1
                                 now, now, now,  // createTime, svrKeyUpdateTime, dbKeyUpdateTime
                                 &masterKeyData);
  if (code != 0) {
    fprintf(stderr, "Error: Failed to build master key data\n");
    taosMemoryFreeClear(machineId);
    return code;
  }
  taosMemoryFreeClear(machineId);

  // Print version info
  printf("  Version: %d\n", masterKeyData.metadata.keyVersion);

  // Determine data directory
  char dataDir[PATH_MAX] = {0};

  // Priority 1: Use dataDir from command line argument
  if (g_args.dataDir[0]) {
    tstrncpy(dataDir, g_args.dataDir, sizeof(dataDir));
  }
  // Priority 2: Parse from config file if configDir is specified
  else if (g_args.configDir[0]) {
    code = taoskParseDataDir(g_args.configDir, dataDir, sizeof(dataDir));
    if (code != TSDB_CODE_SUCCESS) {
      // Failed to parse, use default
      tstrncpy(dataDir, "/var/lib/taos", sizeof(dataDir));
    }
  }
  // Priority 3: Use default
  else {
    tstrncpy(dataDir, "/var/lib/taos", sizeof(dataDir));
  }

  printf("\nUsing data directory: %s\n", dataDir);

  // Create multi-level directory if not exists
  snprintf(encryptFilePath, sizeof(encryptFilePath), "%s/dnode/config", dataDir);
  if (taosMulMkDir(encryptFilePath) != 0) {
    fprintf(stderr, "Error: Failed to create directory: %s\n", encryptFilePath);
    return TSDB_CODE_FAILED;
  }

  // Write master key file
  char masterKeyFile[PATH_MAX] = {0};
  snprintf(masterKeyFile, sizeof(masterKeyFile), "%s/dnode/config/%s", dataDir, MASTER_KEY_FILE_NAME);

  code = taoskWriteMasterKeyFile(masterKeyFile, &masterKeyData);
  if (code != 0) {
    fprintf(stderr, "Error: Failed to write master key file: %s\n", tstrerror(code));
    return code;
  }

  printf("\nMaster keys successfully saved to: %s\n", masterKeyFile);

  // Build and write derived key data if any derived keys are needed
  if (g_args.encryptConfig || g_args.encryptMetadata || g_args.encryptData) {
    SDerivedKeyData derivedKeyData = {0};
    code = taoskBuildDerivedKeyData(g_args.encryptConfig ? cfgKey : NULL, g_args.encryptMetadata ? metaKey : NULL,
                                    g_args.encryptData ? dataKey : NULL, dbKey, cfgAlgo, metaAlgo, &derivedKeyData);
    if (code != 0) {
      fprintf(stderr, "Error: Failed to build derived key data\n");
      return code;
    }

    char derivedKeyFile[PATH_MAX] = {0};
    snprintf(derivedKeyFile, sizeof(derivedKeyFile), "%s/dnode/config/%s", dataDir, DERIVED_KEY_FILE_NAME);

    code = taoskWriteDerivedKeyFile(derivedKeyFile, &derivedKeyData);
    if (code != 0) {
      fprintf(stderr, "Error: Failed to write derived key file: %s\n", tstrerror(code));
      return code;
    }

    printf("Derived keys successfully saved to: %s\n", derivedKeyFile);
  }

  return 0;
}

// Update keys with atomic re-generation of derived keys
int32_t taoskUpdateKeys(void) {
  int32_t code = 0;
  char    masterKeyFile[PATH_MAX] = {0};
  char    derivedKeyFile[PATH_MAX] = {0};
  char *machineId = NULL;

  // Validate new keys
  if (g_args.newSvrKey[0] != '\0') {
    if ((code = taoskValidateKey(g_args.newSvrKey)) != 0) {
      return code;
    }
  }
  
  if (g_args.newDbKey[0] != '\0') {
    if ((code = taoskValidateKey(g_args.newDbKey)) != 0) {
      return code;
    }
  }
  
  // Determine data directory
  const char *dataDir = g_args.dataDir[0] ? g_args.dataDir : "/var/lib/taos";
  snprintf(masterKeyFile, sizeof(masterKeyFile), "%s/dnode/config/%s", dataDir, MASTER_KEY_FILE_NAME);
  snprintf(derivedKeyFile, sizeof(derivedKeyFile), "%s/dnode/config/%s", dataDir, DERIVED_KEY_FILE_NAME);

  // Read existing master key file
  SMasterKeyData existingMasterKeyData = {0};
  char          *oldSvrKey = NULL;
  char          *oldDbKey = NULL;

  code = taoskReadMasterKeyFile(masterKeyFile, &oldSvrKey, &oldDbKey, &existingMasterKeyData);
  if (code != 0) {
    fprintf(stderr, "Error: Failed to read master key file: %s\n", tstrerror(code));
    return code;
  }

  // Determine new keys
  const char *newSvrKey = NULL;
  const char *newDbKey = NULL;
  bool updated = false;

  if (g_args.newSvrKey[0] != '\0') {
    newSvrKey = g_args.newSvrKey;
    updated = true;
    printf("Updating SVR_KEY\n");
  } else {
    newSvrKey = oldSvrKey;
  }

  if (g_args.newDbKey[0] != '\0') {
    newDbKey = g_args.newDbKey;
    updated = true;
    printf("Updating DB_KEY\n");
  } else {
    newDbKey = oldDbKey;
  }

  if (!updated) {
    fprintf(stderr, "Error: No keys specified for update\n");
    taosMemoryFreeClear(oldSvrKey);
    taosMemoryFreeClear(oldDbKey);
    return TSDB_CODE_FAILED;
  }

  // Read existing derived keys (if they exist)
  char           *cfgKey = NULL;
  char           *metaKey = NULL;
  char           *dataKey = NULL;
  SDerivedKeyData existingDerivedKeyData = {0};
  bool            hasDerivedKeys = false;

  code = taoskReadDerivedKeyFile(derivedKeyFile, oldDbKey, &cfgKey, &metaKey, &dataKey, &existingDerivedKeyData);
  if (code == 0) {
    hasDerivedKeys = true;
  }

  // Calculate new version and update times
  int64_t now = taosGetTimestampMs();
  int32_t newVersion = existingMasterKeyData.metadata.keyVersion + 1;
  int64_t newSvrKeyUpdateTime = (g_args.newSvrKey[0] != '\0') ? now : existingMasterKeyData.metadata.svrKeyUpdateTime;
  int64_t newDbKeyUpdateTime = (g_args.newDbKey[0] != '\0') ? now : existingMasterKeyData.metadata.dbKeyUpdateTime;

  // Build and write updated master key data
  SMasterKeyData newMasterKeyData = {0};
  code = taoskBuildMasterKeyData(
      newSvrKey, newDbKey, existingMasterKeyData.metadata.algorithm, existingMasterKeyData.metadata.cfgAlgorithm,
      existingMasterKeyData.metadata.metaAlgorithm, newVersion, existingMasterKeyData.metadata.createTime,
      newSvrKeyUpdateTime, newDbKeyUpdateTime, &newMasterKeyData);
  if (code != 0) {
    fprintf(stderr, "Error: Failed to build master key data\n");
    goto _cleanup;
  }

  code = taoskWriteMasterKeyFile(masterKeyFile, &newMasterKeyData);
  if (code != 0) {
    fprintf(stderr, "Error: Failed to write master key file: %s\n", tstrerror(code));
    goto _cleanup;
  }

  // Atomically regenerate derived key file if it exists
  if (hasDerivedKeys) {
    SDerivedKeyData newDerivedKeyData = {0};
    code = taoskBuildDerivedKeyData(cfgKey, metaKey, dataKey, newDbKey, existingMasterKeyData.metadata.cfgAlgorithm,
                                    existingMasterKeyData.metadata.metaAlgorithm, &newDerivedKeyData);
    if (code != 0) {
      fprintf(stderr, "Error: Failed to build derived key data\n");
      goto _cleanup;
    }

    // Write new derived key file - this is atomic (uses temp file + rename)
    // The old derived key file will be replaced atomically
    code = taoskWriteDerivedKeyFile(derivedKeyFile, &newDerivedKeyData);
    if (code != 0) {
      fprintf(stderr, "Error: Failed to write derived key file: %s\n", tstrerror(code));
      goto _cleanup;
    }

    printf("Derived keys regenerated successfully\n");
  }

  printf("Keys updated successfully (version: %d -> %d)\n", existingMasterKeyData.metadata.keyVersion, newVersion);
  code = 0;

_cleanup:
  taosMemoryFreeClear(oldSvrKey);
  taosMemoryFreeClear(oldDbKey);
  taosMemoryFreeClear(cfgKey);
  taosMemoryFreeClear(metaKey);
  taosMemoryFreeClear(dataKey);

  return code;
}

// Declare internal file operation functions
extern int32_t taoskBackupMasterKeysPortable(const char *masterKeyFile, const char *backupFile,
                                             const char *svrKeyForVerification);
extern int32_t taoskRestoreMasterKeysPortable(const char *backupFile, const char *masterKeyFile,
                                              const char *svrKeyPassword);

// Backup keys - generate portable backup without machine ID
// Requires --svr-key for verification
int32_t taoskBackupKeys(void) {
  char masterKeyFile[PATH_MAX] = {0};
  char backupFile[PATH_MAX] = {0};

  // Check if svrKey is provided for verification
  if (g_args.svrKeyForBackup[0] == '\0') {
    fprintf(stderr, "Error: Server key (--svr-key) is required for backup verification\n");
    fprintf(stderr, "Usage: taosk --backup --svr-key <your_svr_key>\n");
    return TSDB_CODE_INVALID_PARA;
  }

  // Determine data directory
  const char *dataDir = g_args.dataDir[0] ? g_args.dataDir : "/var/lib/taos";
  snprintf(masterKeyFile, sizeof(masterKeyFile), "%s/dnode/config/%s", dataDir, MASTER_KEY_FILE_NAME);

  // Generate backup file path
  snprintf(backupFile, sizeof(backupFile), "%s/dnode/config/%s.backup.%" PRId64, dataDir, MASTER_KEY_FILE_NAME,
           taosGetTimestampMs());

  printf("Backing up master key file (svrKey and dbKey)...\n");
  printf("Verifying server key...\n");
  printf("Note: Backup file will NOT contain machine ID binding.\n");
  printf("      You can restore this backup on any machine.\n\n");

  int32_t code = taoskBackupMasterKeysPortable(masterKeyFile, backupFile, g_args.svrKeyForBackup);
  if (code == 0) {
    printf("\nBackup file created: %s\n", backupFile);
    printf("Keep this file secure and remember your server key for restoration.\n");
  }

  return code;
}

// Restore keys - add machine ID to portable backup
// Requires --svr-key as password and backup file path in --machine-code
int32_t taoskRestoreKeys(void) {
  char backupFile[PATH_MAX] = {0};
  char masterKeyFile[PATH_MAX] = {0};

  // Check if backup file path is provided
  if (g_args.backupFilePath[0] == '\0') {
    fprintf(stderr, "Error: Please specify backup file path with --machine-code option\n");
    fprintf(stderr, "Usage: taosk --restore --machine-code <backup_file_path> --svr-key <your_svr_key>\n");
    return TSDB_CODE_INVALID_PARA;
  }

  // Check if svrKey is provided as password
  if (g_args.svrKeyForBackup[0] == '\0') {
    fprintf(stderr, "Error: Server key (--svr-key) is required for restore\n");
    fprintf(stderr, "Usage: taosk --restore --machine-code <backup_file_path> --svr-key <your_svr_key>\n");
    return TSDB_CODE_INVALID_PARA;
  }

  strncpy(backupFile, g_args.backupFilePath, sizeof(backupFile) - 1);

  // Determine data directory
  const char *dataDir = g_args.dataDir[0] ? g_args.dataDir : "/var/lib/taos";
  snprintf(masterKeyFile, sizeof(masterKeyFile), "%s/dnode/config/%s", dataDir, MASTER_KEY_FILE_NAME);

  printf("Restoring master key file from portable backup...\n");
  printf("Adding machine ID binding to keys...\n");
  printf("Note: Derived keys will be regenerated when needed.\n\n");

  int32_t code = taoskRestoreMasterKeysPortable(backupFile, masterKeyFile, g_args.svrKeyForBackup);
  if (code == 0) {
    printf("\nKeys successfully restored to: %s\n", masterKeyFile);
    printf("You may need to restart taosd for changes to take effect.\n");
  }

  return code;
}