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

#ifndef _TD_COMMON_TENCRYPT_H_
#define _TD_COMMON_TENCRYPT_H_

#include "tdef.h"

#ifdef __cplusplus
extern "C" {
#endif

// ============================================================================
// Encrypted File Operations
// ============================================================================

// File encryption magic number and constants
#define TD_ENCRYPT_FILE_MAGIC   "tdEncrypt"
#define TD_ENCRYPT_FILE_VERSION 1
#define TD_ENCRYPT_MAGIC_LEN    16

// Database encryption status
typedef enum {
  TD_DB_ENCRYPT_STATUS_UNKNOWN = 0,   // Unknown - encryption state uncertain (upgrade scenario)
  TD_DB_ENCRYPT_STATUS_PLAIN = 1,     // Plain - database files are unencrypted
  TD_DB_ENCRYPT_STATUS_ENCRYPTED = 2  // Encrypted - database files are fully encrypted
} ETdDbEncryptStatus;

// DNode encryption status
typedef enum {
  TD_DNODE_ENCRYPT_STATUS_PLAIN = 0,     // Plain - config and metadata files are unencrypted
  TD_DNODE_ENCRYPT_STATUS_ENCRYPTED = 1  // Encrypted - all related files are encrypted
} ETdDnodeEncryptStatus;

// Encrypt file header structure (plaintext part at file beginning)
typedef struct {
  char    magic[TD_ENCRYPT_MAGIC_LEN];  // Magic number "tdEncrypt"
  int32_t algorithm;                    // Encryption algorithm (e.g., TSDB_ENCRYPT_ALGO_SM4 = 1)
  int32_t version;                      // File format version
  int32_t dataLen;                      // Length of encrypted data following header
  char    reserved[32];                 // Reserved for future use
} STdEncryptFileHeader;

/**
 * @brief Write file with encryption header using atomic file replacement
 *
 * This function writes data to a file with an encryption header at the beginning.
 * The caller is responsible for encrypting the data before passing it to this function.
 * It uses atomic file replacement strategy: writes to temp file, then renames.
 *
 * @param filepath Target file path
 * @param algorithm Encryption algorithm identifier (e.g., TSDB_ENCRYPT_ALGO_SM4)
 * @param data Data buffer to write (caller should encrypt data if needed, can be NULL for empty file)
 * @param dataLen Length of data to write (0 for empty file)
 * @return 0 on success, error code on failure
 */
int32_t taosWriteEncryptFileHeader(const char *filepath, int32_t algorithm, const void *data, int32_t dataLen);

/**
 * @brief Read encryption header from file
 *
 * Reads and validates the encryption header from the beginning of a file.
 *
 * @param filepath File path to read
 * @param header Output parameter for header data
 * @return 0 on success, error code on failure
 */
int32_t taosReadEncryptFileHeader(const char *filepath, STdEncryptFileHeader *header);

/**
 * @brief Check if file has encryption header
 *
 * Quickly checks if a file begins with the encryption magic number.
 *
 * @param filepath File path to check
 * @param algorithm Output parameter for algorithm (can be NULL)
 * @return true if file is encrypted, false otherwise
 */
bool taosIsEncryptedFile(const char *filepath, int32_t *algorithm);

/**
 * @brief Write configuration file with encryption support
 *
 * Writes a configuration file with optional encryption based on tsCfgKey.
 * If tsCfgKey is enabled, encrypts the data using SM4 CBC algorithm and
 * writes it with an encryption header. Otherwise, writes the file normally.
 *
 * This function is used for transparent encryption of configuration files
 * including dnode.json, mnode.json, vnode.json, snode.json, raft_config.json,
 * raft_store.json, current.json, vnodes.json, and wal/meta-ver* files.
 *
 * @param filepath Target file path
 * @param data Data buffer to write
 * @param dataLen Length of data to write
 * @return 0 on success, error code on failure
 */
int32_t taosWriteCfgFile(const char *filepath, const void *data, int32_t dataLen);

/**
 * @brief Read configuration file with automatic decryption support.
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
 * @param dataLen Output parameter for data length
 * @return 0 on success, error code on failure
 */
int32_t taosReadCfgFile(const char *filepath, char **data, int32_t *dataLen);

#ifdef __cplusplus
}
#endif

#endif /*_TD_COMMON_TENCRYPT_H_*/

