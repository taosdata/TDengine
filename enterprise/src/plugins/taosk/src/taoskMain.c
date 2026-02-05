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
#include <getopt.h>

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
      // Failed to parse, use default
      tstrncpy(g_args.dataDir, "/var/lib/taos", sizeof(g_args.dataDir));
    }
  }
  // Priority 3: Use default
  else {
    tstrncpy(g_args.dataDir, "/var/lib/taos", sizeof(g_args.dataDir));
  }

  return TSDB_CODE_SUCCESS;
}

int32_t taoskParseArgs(int argc, char *argv[]) {
  static struct option long_options[] = {{"config-dir", required_argument, 0, 'c'},
                                         {"data-dir", required_argument, 0, 'd'},
                                         {"help", no_argument, 0, 'h'},
                                         {"version", no_argument, 0, 'V'},
                                         {"set-cfg-algorithm", required_argument, 0, 1013},
                                         {"set-meta-algorithm", required_argument, 0, 1014},
                                         {"encrypt-server", optional_argument, 0, 1002},
                                         {"encrypt-database", optional_argument, 0, 1003},
                                         {"encrypt-config", no_argument, 0, 1004},
                                         {"encrypt-metadata", no_argument, 0, 1005},
                                         {"encrypt-data", optional_argument, 0, 1006},
                                         {"update-svrkey", required_argument, 0, 1007},
                                         {"update-dbkey", required_argument, 0, 1008},
                                         {"backup", no_argument, 0, 1009},
                                         {"restore", no_argument, 0, 1010},
                                         {"machine-code", required_argument, 0, 1011},
                                         {"svr-key", required_argument, 0, 1012},
                                         {"view-config", required_argument, 0, 1015},
                                         {0, 0, 0, 0}};

  // Initialize default values
  strncpy(g_args.configDir, "/etc/taos", sizeof(g_args.configDir) - 1);
  g_args.cfgAlgorithm = ENCRYPT_ALGO_SM4;   // Default to SM4
  g_args.metaAlgorithm = ENCRYPT_ALGO_SM4;  // Default to SM4
  g_args.generateKeys = false;
  g_args.updateKeys = false;
  g_args.backup = false;
  g_args.restore = false;
  g_args.viewConfig = false;
  g_args.encryptConfig = false;
  g_args.encryptMetadata = false;
  g_args.encryptData = false;
  
  int option_index = 0;
  int c;
  
  while ((c = getopt_long(argc, argv, "c:d:hV", long_options, &option_index)) != -1) {
    switch (c) {
      case 'c':
        strncpy(g_args.configDir, optarg, sizeof(g_args.configDir) - 1);
        break;
      case 'd':
        strncpy(g_args.dataDir, optarg, sizeof(g_args.dataDir) - 1);
        break;
      case 'h':
        g_args.showHelp = true;
        return 0;
      case 'V':
        g_args.showVersion = true;
        return 0;
      case 1013:  // --set-cfg-algorithm
        g_args.cfgAlgorithm = taoskStringToAlgo(optarg);
        if (g_args.cfgAlgorithm == ENCRYPT_ALGO_NONE || g_args.cfgAlgorithm >= ENCRYPT_ALGO_MAX) {
          fprintf(stderr, "Error: Invalid cfg algorithm '%s'. Supported: sm4, aes\n", optarg);
          return -1;
        }
        // Validate that only SM4 or AES is used for cfg
        if (taoskValidateSymmetricAlgo(g_args.cfgAlgorithm) != TSDB_CODE_SUCCESS) {
          return -1;
        }
        break;
      case 1014:  // --set-meta-algorithm
        g_args.metaAlgorithm = taoskStringToAlgo(optarg);
        if (g_args.metaAlgorithm == ENCRYPT_ALGO_NONE || g_args.metaAlgorithm >= ENCRYPT_ALGO_MAX) {
          fprintf(stderr, "Error: Invalid meta algorithm '%s'. Supported: sm4, aes\n", optarg);
          return -1;
        }
        // Validate that only SM4 or AES is used for meta
        if (taoskValidateSymmetricAlgo(g_args.metaAlgorithm) != TSDB_CODE_SUCCESS) {
          return -1;
        }
        break;
      case 1002:  // --encrypt-server
        printf("----> --encrypt-server optarg: %s\n", optarg);
        if (optarg != NULL) {
          // Using --encrypt-server=value format
          if (taoskValidateKey(optarg) != 0) {
            return -1;
          }
          strncpy(g_args.svrKey, optarg, ENCRYPT_KEY_LEN);
        } else if (optind < argc && argv[optind][0] != '-') {
          // Using --encrypt-server value format (with space)
          if (taoskValidateKey(argv[optind]) != 0) {
            return -1;
          }
          strncpy(g_args.svrKey, argv[optind], sizeof(g_args.svrKey) - 1);
          optind++;  // Move to next argument
        }
        // If neither, svrKey remains empty and will be auto-generated
        g_args.generateKeys = true;
        break;
      case 1003:  // --encrypt-database
        if (optarg != NULL) {
          // Using --encrypt-database=value format
          if (taoskValidateKey(optarg) != 0) {
            return -1;
          }
          strncpy(g_args.dbKey, optarg, ENCRYPT_KEY_LEN);
        } else if (optind < argc && argv[optind][0] != '-') {
          // Using --encrypt-database value format (with space)
          if (taoskValidateKey(argv[optind]) != 0) {
            return -1;
          }
          strncpy(g_args.dbKey, argv[optind], sizeof(g_args.dbKey) - 1);
          optind++;  // Move to next argument
        }
        // If neither, dbKey remains empty and will be auto-generated
        g_args.generateKeys = true;
        break;
      case 1004:  // --encrypt-config
        // Check if user tries to provide an argument
        if (optind < argc && argv[optind][0] != '-') {
          fprintf(stderr, "Error: --encrypt-config does not accept arguments (got '%s')\n", argv[optind]);
          fprintf(stderr, "       Config key will be automatically generated\n");
          return -1;
        }
        g_args.encryptConfig = true;
        g_args.generateKeys = true;
        break;
      case 1005:  // --encrypt-metadata
        // Check if user tries to provide an argument
        if (optind < argc && argv[optind][0] != '-') {
          fprintf(stderr, "Error: --encrypt-metadata does not accept arguments (got '%s')\n", argv[optind]);
          fprintf(stderr, "       Metadata key will be automatically generated\n");
          return -1;
        }
        g_args.encryptMetadata = true;
        g_args.generateKeys = true;
        break;
      case 1006:  // --encrypt-data
        if (optarg != NULL) {
          // Using --encrypt-data=value format
          if (taoskValidateKey(optarg) != 0) {
            return -1;
          }
          strncpy(g_args.dataKey, optarg, ENCRYPT_KEY_LEN);
        } else if (optind < argc && argv[optind][0] != '-') {
          // Using --encrypt-data value format (with space)
          if (taoskValidateKey(argv[optind]) != 0) {
            return -1;
          }
          strncpy(g_args.dataKey, argv[optind], sizeof(g_args.dataKey) - 1);
          optind++;  // Move to next argument
        }
        // If neither, dataKey remains empty and will be auto-generated
        g_args.encryptData = true;
        g_args.generateKeys = true;
        break;
      case 1007:  // --update-svrkey
        if (taoskValidateKey(optarg) != 0) {
          return -1;
        }
        strncpy(g_args.newSvrKey, optarg, ENCRYPT_KEY_LEN);
        g_args.updateKeys = true;
        break;
      case 1008:  // --update-dbkey
        if (taoskValidateKey(optarg) != 0) {
          return -1;
        }
        strncpy(g_args.newDbKey, optarg, ENCRYPT_KEY_LEN);
        g_args.updateKeys = true;
        break;
      case 1009:  // --backup
        g_args.backup = true;
        break;
      case 1010:  // --restore
        g_args.restore = true;
        break;
      case 1011:  // --machine-code
        strncpy(g_args.backupFilePath, optarg, sizeof(g_args.backupFilePath) - 1);
        break;
      case 1012:  // --svr-key
        if (taoskValidateKey(optarg) != 0) {
          return -1;
        }
        strncpy(g_args.svrKeyForBackup, optarg, ENCRYPT_KEY_LEN);
        break;
      case 1015:  // --view-config
        strncpy(g_args.configFilePath, optarg, sizeof(g_args.configFilePath) - 1);
        g_args.viewConfig = true;
        break;
      default:
        fprintf(stderr, "Error: Unknown option\n");
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
  }

  taosCloseLog();
  return code == 0 ? 0 : -1;
}

