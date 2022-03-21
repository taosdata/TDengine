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

#ifndef _TD_TFS_H_
#define _TD_TFS_H_

#include "tdef.h"
#include "monitor.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------ TYPES EXPOSED ------------------------ */
typedef struct STfs STfs;
typedef struct STfsDir STfsDir;

typedef struct {
  int32_t level;
  int32_t id;
} SDiskID;

typedef struct {
  SDiskID did;
  char    aname[TSDB_FILENAME_LEN];  // TABS name
  char    rname[TSDB_FILENAME_LEN];  // REL name
  STfs   *pTfs;
} STfsFile;

/**
 * @brief Open a fs.
 *
 * @param pCfg Config of the fs.
 * @param ndisk Length of the config.
 * @return STfs* The fs object.
 */
STfs *tfsOpen(SDiskCfg *pCfg, int32_t ndisk);

/**
 * @brief Close a fs.
 *
 * @param pTfs The fs object to close.
 */
void tfsClose(STfs *pTfs);

/**
 * @brief Update the disk size.
 *
 * @param pTfs The fs object.
 */
void tfsUpdateSize(STfs *pTfs);

/**
 * @brief Get the disk size.
 *
 * @param pTfs The fs object.
 */
SDiskSize tfsGetSize(STfs *pTfs);

/**
 * @brief Allocate an existing available tier level from fs.
 *
 * @param pTfs The fs object.
 * @param expLevel Disk level want to allocate.
 * @param pDiskId The disk ID after allocation.
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t tfsAllocDisk(STfs *pTfs, int32_t expLevel, SDiskID *pDiskId);

/**
 * @brief Get the primary path.
 *
 * @param pTfs The fs object.
 * @return const char * The primary path.
 */
const char *tfsGetPrimaryPath(STfs *pTfs);

/**
 * @brief Get the disk path.
 *
 * @param pTfs The fs object.
 * @param diskId The diskId.
 * @return const char * The primary path.
 */
const char *tfsGetDiskPath(STfs *pTfs, SDiskID diskId);

/**
 * @brief Make directory at all levels in tfs.
 *
 * @param pTfs The fs object.
 * @param rname The rel name of directory.
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t tfsMkdir(STfs *pTfs, const char *rname);

/**
 * @brief Create directories in tfs.
 *
 * @param pTfs The fs object.
 * @param rname The rel name of directory.
 * @param diskId The disk ID.
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t tfsMkdirAt(STfs *pTfs, const char *rname, SDiskID diskId);

/**
 * @brief Recursive create directories in tfs.
 *
 * @param pTfs The fs object.
 * @param rname The rel name of directory.
 * @param diskId The disk ID.
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t tfsMkdirRecurAt(STfs *pTfs, const char *rname, SDiskID diskId);

/**
 * @brief Remove directory at all levels in tfs.
 *
 * @param pTfs The fs object.
 * @param rname The rel name of directory.
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t tfsRmdir(STfs *pTfs, const char *rname);

/**
 * @brief Rename file/directory in tfs.
 *
 * @param pTfs The fs object.
 * @param orname The rel name of old file.
 * @param nrname The rel name of new file.
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t tfsRename(STfs *pTfs, char *orname, char *nrname);

/**
 * @brief Init file object in tfs.
 *
 * @param pTfs The fs object.
 * @param pFile The file object.
 * @param diskId The disk ID.
 * @param rname The rel name of file.
 */
void tfsInitFile(STfs *pTfs, STfsFile *pFile, SDiskID diskId, const char *rname);

/**
 * @brief Determine whether they are the same file.
 *
 * @param pFile1 The file object.
 * @param pFile2 The file object.
 * @param bool The compare result.
 */
bool tfsIsSameFile(const STfsFile *pFile1, const STfsFile *pFile2);

/**
 * @brief Encode file name to a buffer.
 *
 * @param buf The buffer where file name are saved.
 * @param pFile The file object.
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t tfsEncodeFile(void **buf, STfsFile *pFile);

/**
 * @brief Decode file name from a buffer.
 *
 * @param pTfs The fs object.
 * @param buf The buffer where file name are saved.
 * @param pFile The file object.
 * @return void * Buffer address after decode.
 */
void *tfsDecodeFile(STfs *pTfs, void *buf, STfsFile *pFile);

/**
 * @brief Get the basename of the file.
 *
 * @param pFile The file object.
 * @param dest The buffer where basename will be saved.
 */
void tfsBasename(const STfsFile *pFile, char *dest);

/**
 * @brief Get the dirname of the file.
 *
 * @param pFile The file object.
 * @param dest The buffer where dirname will be saved.
 */
void tfsDirname(const STfsFile *pFile, char *dest);

/**
 * @brief Get the absolute file name of rname.
 *
 * @param pTfs
 * @param diskId
 * @param rname relative file name
 * @param aname absolute file name
 */
void tfsAbsoluteName(STfs *pTfs, SDiskID diskId, const char *rname, char *aname);

/**
 * @brief Remove file in tfs.
 *
 * @param pFile The file to be removed.
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t tfsRemoveFile(const STfsFile *pFile);

/**
 * @brief Copy file in tfs.
 *
 * @param pFile1 The src file.
 * @param pFile2 The dest file.
 * @return int32_t 0 for success, -1 for failure.
 */
int32_t tfsCopyFile(const STfsFile *pFile1, const STfsFile *pFile2);

/**
 * @brief Open a directory for traversal.
 *
 * @param rname The rel name of file.
 * @return STfsDir* The dir object.
 */
STfsDir *tfsOpendir(STfs *pTfs, const char *rname);

/**
 * @brief Get a file from dir and move to next pos.
 *
 * @param pDir The dir object.
 * @return STfsFile* The file in dir.
 */
const STfsFile *tfsReaddir(STfsDir *pDir);

/**
 * @brief Close a directory.
 *
 * @param pDir The dir object.
 */
void tfsClosedir(STfsDir *pDir);

/**
 * @brief Get disk info of tfs.
 *
 * @param pTfs The fs object.
 * @param pInfo The info object.
 */
int32_t tfsGetMonitorInfo(STfs *pTfs, SMonDiskInfo *pInfo);

#ifdef __cplusplus
}
#endif

#endif /*_TD_TFS_H_*/
