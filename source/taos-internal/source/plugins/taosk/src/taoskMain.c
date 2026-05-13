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

STaoskArgs g_args = {0};

void taoskPrintHelp(void) {
  printf("Usage: taosk [OPTIONS]\n\n");
  printf("TDengine Storage Security Key Management Tool v%s\n\n", TAOSK_VERSION);
  
  printf("Options:\n");
  printf("  -c, --config-dir <dir>           Configuration directory (default: /etc/taos)\n");
  printf("  -d, --data-dir <dir>             Data directory (default: from config)\n");
  printf("  -h, --help                       Show this help message\n");
  printf("  -V, --version                    Show version information\n");
  printf("\n");
  
  printf("Key Generation:\n");
  printf("  --set-cfg-algorithm <algo>       Set encryption algorithm for CFG_KEY (sm4|aes, default: sm4)\n");
  printf("  --set-meta-algorithm <algo>      Set encryption algorithm for META_KEY (sm4|aes, default: sm4)\n");
  printf("  --encrypt-server [key]           Enable server encryption (auto-generate or specify SVR_KEY)\n");
  printf("  --encrypt-database [key]         Enable database encryption (auto-generate or specify DB_KEY)\n");
  printf("  --encrypt-config                 Enable config encryption (auto-generate CFG_KEY)\n");
  printf("  --encrypt-metadata               Enable metadata encryption (auto-generate META_KEY)\n");
  printf("  --encrypt-data [key]             Enable data encryption (auto-generate or specify DATA_KEY)\n");
  printf("\n");
  
  printf("Key Update:\n");
  printf("  --update-svrkey <key>            Update server key\n");
  printf("  --update-dbkey <key>             Update database key\n");
  printf("\n");
  
  printf("Backup/Restore:\n");
  printf("  --backup                         Backup keys (creates portable backup without machine ID)\n");
  printf("  --restore                        Restore keys (adds current machine ID to portable backup)\n");
  printf("  --machine-code <path>            Backup file path (used with --restore)\n");
  printf("  --svr-key <key>                  Server key for verification (--backup) or decryption (--restore)\n");
  printf("\n");

  printf("View Encrypted Config:\n");
  printf("  --view-config <file>             View encrypted configuration file\n");
  printf("                                   (requires keys loaded from data directory)\n");
  printf("\n");

  printf("Edit Encrypted Config:\n");
  printf("  --edit-file <file>               Edit encrypted configuration file\n");
  printf("                                   (requires keys loaded from data directory)\n");
  printf("                                   Uses $EDITOR environment variable or vi\n");
  printf("\n");

  printf("Examples:\n");
  printf("  # Generate keys with default algorithm (SM4)\n");
  printf("  taosk -c /etc/taos --encrypt-server mykey123 --encrypt-database dbkey456\n");
  printf("\n");
  printf("  # Generate keys with specific algorithm and enable encryption\n");
  printf("  taosk -c /etc/taos --set-algorithm sm4 --encrypt-server svr_key \\\n");
  printf("        --encrypt-database db_key --encrypt-config --encrypt-metadata \\\n");
  printf("        --encrypt-data data_key\n");
  printf("\n");
  printf("  # Update keys\n");
  printf("  taosk -c /etc/taos --update-svrkey new_svr_key --update-dbkey new_db_key\n");
  printf("\n");
  printf("  # Backup keys (requires server key verification)\n");
  printf("  taosk -c /etc/taos --backup --svr-key mykey123\n");
  printf("  # This creates a portable backup file without machine ID binding\n");
  printf("\n");
  printf("  # Restore keys on any machine (requires server key and backup file)\n");
  printf("  taosk -c /etc/taos --restore --machine-code /path/to/backup_file \\\n");
  printf("        --svr-key mykey123\n");
  printf("  # This adds current machine ID binding to the keys\n");
  printf("\n");
  printf("  # View encrypted config file\n");
  printf("  taosk -d /var/lib/taos --view-config /path/to/encrypted_config.json\n");
  printf("  # This loads keys from data directory and decrypts the config file\n");
  printf("\n");
  printf("  # Edit encrypted config file\n");
  printf("  taosk -d /var/lib/taos --edit-file /path/to/encrypted_config.json\n");
  printf("  # Opens file in editor, saves changes encrypted\n");
  printf("\n");
}

void taoskPrintVersion(void) {
  printf("taosk version %s\n", TAOSK_VERSION);
  printf("TDengine Storage Security Key Management Tool\n");
}

/**
 * Determine data directory based on command line args and config file
 * This is the centralized logic used by all operations
 *
 * Priority order:
 * 1. Use dataDir from command line argument (-d/--data-dir)
 * 2. Parse from config file if configDir is specified (-c/--config-dir)
 * 3. Use default: /var/lib/taos
 *
 * @return 0 on success, error code on failure
 */
static int32_t taoskDetermineDataDir(void) {
  int32_t code = 0;

  // Priority 1: Use dataDir from command line argument
  if (g_args.dataDir[0]) {
    // Already specified, nothing to do
    return TSDB_CODE_SUCCESS;
  }

  // Priority 2: Parse from config file if configDir is specified
  if (g_args.configDir[0]) {
    code = taoskParseDataDir(g_args.configDir, g_args.dataDir, sizeof(g_args.dataDir));
    if (code != TSDB_CODE_SUCCESS) {
// Failed to parse, use platform default
#ifdef WINDOWS
      tstrncpy(g_args.dataDir, "C:\\TDengine\\data", sizeof(g_args.dataDir));
#else
      tstrncpy(g_args.dataDir, "/var/lib/taos", sizeof(g_args.dataDir));
#endif
    }
  }
  // Priority 3: Use platform default
  else {
#ifdef WINDOWS
    tstrncpy(g_args.dataDir, "C:\\TDengine\\data", sizeof(g_args.dataDir));
#else
    tstrncpy(g_args.dataDir, "/var/lib/taos", sizeof(g_args.dataDir));
#endif
  }

  return TSDB_CODE_SUCCESS;
}

// Cross-platform argument parsing helper:
// Retrieves the value for a long option, supporting both "--opt=val" and "--opt val" formats.
// For optional-value options, returns NULL if no value follows.
// 'i' is advanced if the value is taken from the next argv element.
static const char *taoskGetOptVal(int *i, int argc, char *argv[], const char *argWithEq, bool required) {
  const char *eq = strchr(argv[*i], '=');
  if (eq != NULL) {
    return eq + 1;
  }
  if (*i + 1 < argc && argv[*i + 1][0] != '-') {
    (*i)++;
    return argv[*i];
  }
  (void)argWithEq;
  (void)required;
  return NULL;
}

// Match a long option name (handles both "--opt" and "--opt=..." forms)
static bool taoskMatchLong(const char *arg, const char *name) {
  size_t len = strlen(name);
  if (strncmp(arg, "--", 2) != 0) return false;
  arg += 2;
  return (strncmp(arg, name, len) == 0 && (arg[len] == '\0' || arg[len] == '='));
}

int32_t taoskParseArgs(int argc, char *argv[]) {
  // Initialize default values
#ifdef WINDOWS
  strncpy(g_args.configDir, "C:\\TDengine\\cfg", sizeof(g_args.configDir) - 1);
#else
  strncpy(g_args.configDir, "/etc/taos", sizeof(g_args.configDir) - 1);
#endif
  g_args.cfgAlgorithm = ENCRYPT_ALGO_SM4;
  g_args.metaAlgorithm = ENCRYPT_ALGO_SM4;
  g_args.generateKeys = false;
  g_args.updateKeys = false;
  g_args.backup = false;
  g_args.restore = false;
  g_args.viewConfig = false;
  g_args.editFile = false;
  g_args.encryptConfig = false;
  g_args.encryptMetadata = false;
  g_args.encryptData = false;

  for (int i = 1; i < argc; i++) {
    const char *arg = argv[i];

    if (strcmp(arg, "-h") == 0 || taoskMatchLong(arg, "help")) {
      g_args.showHelp = true;
      return 0;
    } else if (strcmp(arg, "-V") == 0 || taoskMatchLong(arg, "version")) {
      g_args.showVersion = true;
      return 0;
    } else if (strcmp(arg, "-c") == 0 || taoskMatchLong(arg, "config-dir")) {
      const char *val = (arg[1] == '-') ? taoskGetOptVal(&i, argc, argv, arg, true)
                                        : (i + 1 < argc ? argv[++i] : NULL);
      if (!val) {
        fprintf(stderr, "Error: %s requires an argument\n", arg);
        return -1;
      }
      strncpy(g_args.configDir, val, sizeof(g_args.configDir) - 1);
    } else if (strcmp(arg, "-d") == 0 || taoskMatchLong(arg, "data-dir")) {
      const char *val = (arg[1] == '-') ? taoskGetOptVal(&i, argc, argv, arg, true)
                                        : (i + 1 < argc ? argv[++i] : NULL);
      if (!val) {
        fprintf(stderr, "Error: %s requires an argument\n", arg);
        return -1;
      }
      strncpy(g_args.dataDir, val, sizeof(g_args.dataDir) - 1);
    } else if (taoskMatchLong(arg, "set-cfg-algorithm")) {
      const char *val = taoskGetOptVal(&i, argc, argv, arg, true);
      if (!val) {
        fprintf(stderr, "Error: --set-cfg-algorithm requires an argument\n");
        return -1;
      }
      g_args.cfgAlgorithm = taoskStringToAlgo(val);
      if (g_args.cfgAlgorithm == ENCRYPT_ALGO_NONE || g_args.cfgAlgorithm >= ENCRYPT_ALGO_MAX) {
        fprintf(stderr, "Error: Invalid cfg algorithm '%s'. Supported: sm4, aes\n", val);
        return -1;
      }
      if (taoskValidateSymmetricAlgo(g_args.cfgAlgorithm) != TSDB_CODE_SUCCESS) {
        return -1;
      }
    } else if (taoskMatchLong(arg, "set-meta-algorithm")) {
      const char *val = taoskGetOptVal(&i, argc, argv, arg, true);
      if (!val) {
        fprintf(stderr, "Error: --set-meta-algorithm requires an argument\n");
        return -1;
      }
      g_args.metaAlgorithm = taoskStringToAlgo(val);
      if (g_args.metaAlgorithm == ENCRYPT_ALGO_NONE || g_args.metaAlgorithm >= ENCRYPT_ALGO_MAX) {
        fprintf(stderr, "Error: Invalid meta algorithm '%s'. Supported: sm4, aes\n", val);
        return -1;
      }
      if (taoskValidateSymmetricAlgo(g_args.metaAlgorithm) != TSDB_CODE_SUCCESS) {
        return -1;
      }
    } else if (taoskMatchLong(arg, "encrypt-server")) {
      const char *val = taoskGetOptVal(&i, argc, argv, arg, false);
      if (val != NULL) {
        if (taoskValidateKey(val) != 0) {
          return -1;
        }
        strncpy(g_args.svrKey, val, ENCRYPT_KEY_LEN);
      }
      g_args.generateKeys = true;
    } else if (taoskMatchLong(arg, "encrypt-database")) {
      const char *val = taoskGetOptVal(&i, argc, argv, arg, false);
      if (val != NULL) {
        if (taoskValidateKey(val) != 0) {
          return -1;
        }
        strncpy(g_args.dbKey, val, ENCRYPT_KEY_LEN);
      }
      g_args.generateKeys = true;
    } else if (taoskMatchLong(arg, "encrypt-config")) {
      if (i + 1 < argc && argv[i + 1][0] != '-') {
        fprintf(stderr, "Error: --encrypt-config does not accept arguments (got '%s')\n", argv[i + 1]);
        fprintf(stderr, "       Config key will be automatically generated\n");
        return -1;
      }
      g_args.encryptConfig = true;
      g_args.generateKeys = true;
    } else if (taoskMatchLong(arg, "encrypt-metadata")) {
      if (i + 1 < argc && argv[i + 1][0] != '-') {
        fprintf(stderr, "Error: --encrypt-metadata does not accept arguments (got '%s')\n", argv[i + 1]);
        fprintf(stderr, "       Metadata key will be automatically generated\n");
        return -1;
      }
      g_args.encryptMetadata = true;
      g_args.generateKeys = true;
    } else if (taoskMatchLong(arg, "encrypt-data")) {
      const char *val = taoskGetOptVal(&i, argc, argv, arg, false);
      if (val != NULL) {
        if (taoskValidateKey(val) != 0) {
          return -1;
        }
        strncpy(g_args.dataKey, val, ENCRYPT_KEY_LEN);
      }
      g_args.encryptData = true;
      g_args.generateKeys = true;
    } else if (taoskMatchLong(arg, "update-svrkey")) {
      const char *val = taoskGetOptVal(&i, argc, argv, arg, true);
      if (!val) {
        fprintf(stderr, "Error: --update-svrkey requires an argument\n");
        return -1;
      }
      if (taoskValidateKey(val) != 0) {
        return -1;
      }
      strncpy(g_args.newSvrKey, val, ENCRYPT_KEY_LEN);
      g_args.updateKeys = true;
    } else if (taoskMatchLong(arg, "update-dbkey")) {
      const char *val = taoskGetOptVal(&i, argc, argv, arg, true);
      if (!val) {
        fprintf(stderr, "Error: --update-dbkey requires an argument\n");
        return -1;
      }
      if (taoskValidateKey(val) != 0) {
        return -1;
      }
      strncpy(g_args.newDbKey, val, ENCRYPT_KEY_LEN);
      g_args.updateKeys = true;
    } else if (taoskMatchLong(arg, "backup")) {
      g_args.backup = true;
    } else if (taoskMatchLong(arg, "restore")) {
      g_args.restore = true;
    } else if (taoskMatchLong(arg, "machine-code")) {
      const char *val = taoskGetOptVal(&i, argc, argv, arg, true);
      if (!val) {
        fprintf(stderr, "Error: --machine-code requires an argument\n");
        return -1;
      }
      strncpy(g_args.backupFilePath, val, sizeof(g_args.backupFilePath) - 1);
    } else if (taoskMatchLong(arg, "svr-key")) {
      const char *val = taoskGetOptVal(&i, argc, argv, arg, true);
      if (!val) {
        fprintf(stderr, "Error: --svr-key requires an argument\n");
        return -1;
      }
      if (taoskValidateKey(val) != 0) {
        return -1;
      }
      strncpy(g_args.svrKeyForBackup, val, ENCRYPT_KEY_LEN);
    } else if (taoskMatchLong(arg, "view-config")) {
      const char *val = taoskGetOptVal(&i, argc, argv, arg, true);
      if (!val) {
        fprintf(stderr, "Error: --view-config requires an argument\n");
        return -1;
      }
      strncpy(g_args.configFilePath, val, sizeof(g_args.configFilePath) - 1);
      g_args.viewConfig = true;
    } else if (taoskMatchLong(arg, "edit-file")) {
      const char *val = taoskGetOptVal(&i, argc, argv, arg, true);
      if (!val) {
        fprintf(stderr, "Error: --edit-file requires an argument\n");
        return -1;
      }
      strncpy(g_args.editFilePath, val, sizeof(g_args.editFilePath) - 1);
      g_args.editFile = true;
    } else {
      fprintf(stderr, "Error: Unknown option '%s'\n", arg);
      taoskPrintHelp();
      return -1;
    }
  }

  return 0;
}

int main(int argc, char *argv[]) {
  int32_t code = 0;
  
  // Parse command line arguments
  if (taoskParseArgs(argc, argv) != 0) {
    return -1;
  }
  
  // Show help
  if (g_args.showHelp) {
    taoskPrintHelp();
    return 0;
  }
  
  // Show version
  if (g_args.showVersion) {
    taoskPrintVersion();
    return 0;
  }
  
  // Initialize TDengine environment
  if (taosInitLog("taoskLog", 1, false) != 0) {
    fprintf(stderr, "Error: Failed to initialize log system\n");
    return -1;
  }
  
  // Validate operations
  int opCount = 0;
  if (g_args.generateKeys) opCount++;
  if (g_args.updateKeys) opCount++;
  if (g_args.backup) opCount++;
  if (g_args.restore) opCount++;
  if (g_args.viewConfig) opCount++;
  if (g_args.editFile) opCount++;
  if (opCount == 0) {
    fprintf(stderr, "Error: No operation specified. Use --help for usage information.\n");
    taosCloseLog();
    return -1;
  }
  
  if (opCount > 1) {
    fprintf(stderr, "Error: Multiple operations specified. Please specify only one operation at a time.\n");
    taosCloseLog();
    return -1;
  }

  // Determine data directory (used by all operations)
  code = taoskDetermineDataDir();
  if (code != 0) {
    fprintf(stderr, "Error: Failed to determine data directory: %s\n", tstrerror(code));
    taosCloseLog();
    return -1;
  }
  
  // Execute operation
  if (g_args.viewConfig) {
    printf("Viewing encrypted configuration file...\n");
    code = taoskViewEncryptedConfig();
    if (code == 0) {
      printf("\n");  // Extra newline after content
    } else {
      fprintf(stderr, "Error: Failed to view config file: %s\n", tstrerror(code));
    }
  } else if (g_args.generateKeys) {
    printf("Generating encryption keys...\n");
    code = taoskGenerateKeys();
    if (code == 0) {
      printf("Keys generated successfully.\n");
      printf("Encryption files saved to:\n");
      printf("  - %s/dnode/config/%s (master keys: svrKey, dbKey)\n", g_args.dataDir[0] ? g_args.dataDir : tsDataDir,
             MASTER_KEY_FILE_NAME);
      printf("  - %s/dnode/config/%s (derived keys: cfgKey, metaKey, dataKey)\n",
             g_args.dataDir[0] ? g_args.dataDir : tsDataDir, DERIVED_KEY_FILE_NAME);
    } else {
      fprintf(stderr, "Error: Failed to generate keys: %s\n", tstrerror(code));
    }
  } else if (g_args.updateKeys) {
    printf("Updating encryption keys...\n");
    code = taoskUpdateKeys();
    if (code == 0) {
      printf("Keys updated successfully.\n");
    } else {
      fprintf(stderr, "Error: Failed to update keys: %s\n", tstrerror(code));
    }
  } else if (g_args.backup) {
    printf("Backing up encryption keys...\n");
    code = taoskBackupKeys();
    if (code == 0) {
      printf("Keys backed up successfully.\n");
    } else {
      fprintf(stderr, "Error: Failed to backup keys: %s\n", tstrerror(code));
    }
  } else if (g_args.restore) {
    printf("Restoring encryption keys...\n");
    code = taoskRestoreKeys();
    if (code == 0) {
      printf("Keys restored successfully.\n");
    } else {
      fprintf(stderr, "Error: Failed to restore keys: %s\n", tstrerror(code));
    }
  } else if (g_args.editFile) {
    printf("Editing encrypted configuration file...\n");
    code = taoskEditEncryptedFile();
    if (code == 0) {
      printf("Configuration file edited successfully.\n");
    } else {
      fprintf(stderr, "Error: Failed to edit configuration file: %s\n", tstrerror(code));
    }
  }

  taosCloseLog();
  return code == 0 ? 0 : -1;
}

