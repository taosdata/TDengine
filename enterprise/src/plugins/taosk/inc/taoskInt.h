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

#ifndef _TAOSK_INT_H_
#define _TAOSK_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "taoserror.h"
#include "tdef.h"
#include "tglobal.h"
#include "tgrant.h"
#include "tlog.h"

#define TAOSK_VERSION "1.0.0"
#define MAX_KEY_LEN 128
#define MASTER_KEY_FILE_NAME "master.bin"    // Master keys: svrKey, dbKey
#define DERIVED_KEY_FILE_NAME "derived.bin"  // Derived keys: cfgKey, metaKey, dataKey
#define ENCRYPT_FILE_MAGIC "tdEncrypt"
#define ENCRYPT_FILE_VERSION 1

// Key types
typedef enum {
  KEY_TYPE_SVR = 0,  // Server key
  KEY_TYPE_DB,       // Database key
  KEY_TYPE_CFG,      // Config key
  KEY_TYPE_META,     // Metadata key
  KEY_TYPE_DATA,     // Data key
  KEY_TYPE_MAXP
} ETaoskKeyType;

// Encryption algorithm
typedef enum {
  ENCRYPT_ALGO_NONE = 0,
  ENCRYPT_ALGO_SM4,     // 1: SM4-CBC symmetric encryption
  ENCRYPT_ALGO_AES,     // 2: AES-128-CBC symmetric encryption
  ENCRYPT_ALGO_SM3,     // 3: SM3 digest
  ENCRYPT_ALGO_SHA256,  // 4: SHA-256 digest
  ENCRYPT_ALGO_SM2,     // 5: SM2 asymmetric cipher
  ENCRYPT_ALGO_MAX
} EEncryptAlgo;

// Key entry structure
typedef struct {
  ETaoskKeyType type;
  char key[MAX_KEY_LEN + 1];
  int64_t lastModified;
  bool enabled;
} SKeyEntry;

// Encrypt file header (plaintext part)
typedef struct {
  char magic[16];           // Magic number "tdEncrypt"
  int32_t version;          // File version
  int32_t dataLen;          // Length of encrypted data
  char    reserved[32];     // Reserved for future use
} SEncryptFileHeader;

// Encrypted metadata structure (encrypted with SVR_KEY)
typedef struct {
  int32_t algorithm;        // Encryption algorithm for master keys (SVR_KEY, DB_KEY)
  int32_t cfgAlgorithm;     // Encryption algorithm for CFG_KEY
  int32_t metaAlgorithm;    // Encryption algorithm for META_KEY
  int32_t keyVersion;       // Key version, starts from 1, increments on each update
  int64_t createTime;       // Create timestamp
  int64_t svrKeyUpdateTime; // SVR_KEY last update timestamp
  int64_t dbKeyUpdateTime;  // DB_KEY last update timestamp
  char    reserved[40];     // Reserved for future use
} SEncryptMetadata;

// Key data structure (stored in encrypted form) - Legacy format
typedef struct {
  char             svrKeyEncrypted[MAX_KEY_LEN + 1];   // SVR_KEY encrypted with machine code
  char             dbKeyEncrypted[MAX_KEY_LEN + 1];    // DB_KEY encrypted with SVR_KEY
  char             cfgKeyEncrypted[MAX_KEY_LEN + 1];   // CFG_KEY encrypted with DB_KEY
  char             metaKeyEncrypted[MAX_KEY_LEN + 1];  // META_KEY encrypted with DB_KEY
  char             dataKeyEncrypted[MAX_KEY_LEN + 1];  // DATA_KEY encrypted with DB_KEY
  SEncryptMetadata metadata;                           // Metadata encrypted with SVR_KEY
  bool             cfgKeyEnabled;
  bool             metaKeyEnabled;
  bool             dataKeyEnabled;
} SEncryptedKeyData;

// Master key data structure (svrKey and dbKey only)
typedef struct {
  char             svrKeyEncrypted[MAX_KEY_LEN + 1];  // SVR_KEY encrypted with machine code
  char             dbKeyEncrypted[MAX_KEY_LEN + 1];   // DB_KEY encrypted with SVR_KEY
  SEncryptMetadata metadata;                          // Metadata
} SMasterKeyData;

// Derived key data structure (cfgKey, metaKey, dataKey)
typedef struct {
  char    cfgKeyEncrypted[MAX_KEY_LEN + 1];   // CFG_KEY encrypted with DB_KEY
  char    metaKeyEncrypted[MAX_KEY_LEN + 1];  // META_KEY encrypted with DB_KEY
  char    dataKeyEncrypted[MAX_KEY_LEN + 1];  // DATA_KEY encrypted with DB_KEY
  bool    cfgKeyEnabled;
  bool    metaKeyEnabled;
  bool    dataKeyEnabled;
  int32_t cfgAlgorithm;    // Encryption algorithm for CFG_KEY (SM4 or AES)
  int32_t metaAlgorithm;   // Encryption algorithm for META_KEY (SM4 or AES)
  int64_t generationTime;  // When this derived key file was generated
  char    reserved[32];    // Reserved for future use
} SDerivedKeyData;

// Portable backup data structure (for cross-machine migration)
// Keys are encrypted with user password instead of machine ID
typedef struct {
  char             svrKeyEncrypted[MAX_KEY_LEN + 1];  // SVR_KEY encrypted with user password
  char             dbKeyEncrypted[MAX_KEY_LEN + 1];   // DB_KEY encrypted with user password
  SEncryptMetadata metadata;                          // Metadata (algorithm, version, timestamps)
  int64_t          backupTime;                        // When this backup was created
  char             reserved[32];                      // Reserved for future use
} SPortableBackupData;

// Command line arguments
typedef struct {
  char configDir[PATH_MAX];
  char dataDir[PATH_MAX];
  
  // Key generation
  bool generateKeys;
  char svrKey[MAX_KEY_LEN + 1];
  char dbKey[MAX_KEY_LEN + 1];
  char dataKey[MAX_KEY_LEN + 1];
  bool encryptConfig;
  bool encryptMetadata;
  bool encryptData;
  
  // Algorithm
  EEncryptAlgo cfgAlgorithm;   // Algorithm for CFG_KEY
  EEncryptAlgo metaAlgorithm;  // Algorithm for META_KEY

  // Key update
  bool updateKeys;
  char newSvrKey[MAX_KEY_LEN + 1];
  char newDbKey[MAX_KEY_LEN + 1];
  
  // Backup/Restore
  bool backup;
  bool restore;
  char backupFilePath[PATH_MAX];          // Backup file path for restore operation
  char svrKeyForBackup[MAX_KEY_LEN + 1];  // Backup password for cross-machine migration
  char backupPassword[MAX_KEY_LEN + 1];   // Password for encrypting backup file

  // View/Help
  bool showVersion;
  bool showHelp;
} STaoskArgs;

// Global variables
extern STaoskArgs g_args;

// Function declarations
int32_t taoskParseArgs(int argc, char *argv[]);
void taoskPrintHelp(void);
void taoskPrintVersion(void);

// Key generation and management
int32_t taoskGenerateKeys(void);
int32_t taoskUpdateKeys(void);
int32_t taoskBackupKeys(void);
int32_t taoskRestoreKeys(void);

// File operations
int32_t taoskReadEncryptFile(const char *filepath, SEncryptFileHeader *header, SKeyEntry **keys, int32_t *keyCount);
int32_t taoskWriteEncryptFile(const char *filepath, SEncryptFileHeader *header, SKeyEntry *keys, int32_t keyCount);

// Internal file operation helpers (used by taoskBackupKeys/taoskRestoreKeys)
int32_t taoskBackupFile(const char *srcFile, const char *destFile);
int32_t taoskRestoreFile(const char *srcFile, const char *destFile);

// Portable backup/restore (cross-machine migration without machine ID binding)
// These functions create/restore backups that are not bound to specific machine ID
int32_t taoskBackupMasterKeysPortable(const char *masterKeyFile, const char *backupFile,
                                      const char *svrKeyForVerification);
int32_t taoskRestoreMasterKeysPortable(const char *backupFile, const char *masterKeyFile, const char *svrKeyPassword);

// Internal helper functions for key encryption and file I/O
int32_t taoskBuildEncryptedKeyData(const char *svrKey, const char *dbKey,
                                    const char *cfgKey, const char *metaKey, const char *dataKey,
                                    int32_t algorithm, int32_t keyVersion,
                                    int64_t createTime, int64_t svrKeyUpdateTime, int64_t dbKeyUpdateTime,
                                    SEncryptedKeyData *keyData);
int32_t taoskWriteEncryptedFile(const char *filepath, const SEncryptedKeyData *keyData);

// New split file functions
int32_t taoskBuildMasterKeyData(const char *svrKey, const char *dbKey, int32_t algorithm, int32_t cfgAlgorithm,
                                int32_t metaAlgorithm, int32_t keyVersion, int64_t createTime, int64_t svrKeyUpdateTime,
                                int64_t dbKeyUpdateTime, SMasterKeyData *keyData);
int32_t taoskBuildDerivedKeyData(const char *cfgKey, const char *metaKey, const char *dataKey, const char *dbKey,
                                 int32_t cfgAlgorithm, int32_t metaAlgorithm, SDerivedKeyData *keyData);
int32_t taoskWriteMasterKeyFile(const char *filepath, const SMasterKeyData *keyData);
int32_t taoskWriteDerivedKeyFile(const char *filepath, const SDerivedKeyData *keyData);
int32_t taoskReadMasterKeyFile(const char *filepath, char **svrKey, char **dbKey, SMasterKeyData *keyData);
int32_t taoskReadDerivedKeyFile(const char *filepath, const char *dbKey, char **cfgKey, char **metaKey, char **dataKey,
                                SDerivedKeyData *keyData);

// Functions for taosd integration (load/save keys with version tracking)
//
// taoskLoadEncryptKeys: Load encryption keys from split files (master.bin and derived.bin)
//   - masterKeyFile: Full path to master.bin file
//   - derivedKeyFile: Full path to derived.bin file
//   - fileVersion: file format version for compatibility (from header)
//   - keyVersion: key update version, starts from 1, increments on each update (from metadata)
//
// taoskSaveEncryptKeys: Save encryption keys to split files (master.bin and derived.bin)
//   - masterKeyFile: Full path to master.bin file
//   - derivedKeyFile: Full path to derived.bin file
int32_t taoskLoadEncryptKeys(const char *masterKeyFile, const char *derivedKeyFile, char *svrKey, char *dbKey,
                             char *cfgKey, char *metaKey, char *dataKey, int32_t *algorithm, int32_t *cfgAlgorithm,
                             int32_t *metaAlgorithm, int32_t *fileVersion, int32_t *keyVersion, int64_t *createTime,
                             int64_t *svrKeyUpdateTime, int64_t *dbKeyUpdateTime);
int32_t taoskSaveEncryptKeys(const char *masterKeyFile, const char *derivedKeyFile, const char *svrKey,
                             const char *dbKey, const char *cfgKey, const char *metaKey, const char *dataKey,
                             int32_t algorithm, int32_t cfgAlgorithm, int32_t metaAlgorithm, int32_t keyVersion,
                             int64_t createTime, int64_t svrKeyUpdateTime, int64_t dbKeyUpdateTime);

// Key derivation
int32_t taoskDeriveKeys(const char *svrKey, const char *dbKey, SKeyEntry *keys, int32_t *keyCount);

// Utility functions
const char* taoskAlgoToString(EEncryptAlgo algo);
EEncryptAlgo taoskStringToAlgo(const char *algoStr);
bool         taoskIsSymmetricAlgo(EEncryptAlgo algo);        // Check if algorithm is SM4 or AES
int32_t      taoskValidateSymmetricAlgo(EEncryptAlgo algo);  // Validate algorithm is SM4 or AES only
const char  *taoskKeyTypeToString(ETaoskKeyType type);
int32_t taoskGenerateRandomKey(char *key, int32_t len);
int32_t taoskValidateKey(const char *key);

// Encryption functions (proprietary algorithm)
int32_t taoskEncryptData(const char *plaintext, const char *key, char **ciphertext);
int32_t taoskDecryptData(const char *ciphertext, const char *key, char **plaintext);

#ifdef __cplusplus
}
#endif

#endif  // _TAOSK_INT_H_

