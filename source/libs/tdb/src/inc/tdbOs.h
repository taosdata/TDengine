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

#ifndef _TDB_OS_H_
#define _TDB_OS_H_

#ifdef __cplusplus
extern "C" {
#endif

// TODO: use cmake to control the option
#define TDB_FOR_TDENGINE

#ifdef TDB_FOR_TDENGINE
#include "os.h"
#include "thash.h"

// For memory -----------------
#define tdbOsMalloc  taosMemoryMalloc
#define tdbOsCalloc  taosMemoryCalloc
#define tdbOsRealloc taosMemoryRealloc
#define tdbOsFree    taosMemoryFree

// For file and directory -----------------
/* file */
typedef TdFilePtr tdb_fd_t;

#define TDB_FD_INVALID(fd) (fd == NULL)

#define TDB_O_CREAT  TD_FILE_CREATE
#define TDB_O_WRITE  TD_FILE_WRITE
#define TDB_O_READ   TD_FILE_READ
#define TDB_O_TRUNC  TD_FILE_TRUNC
#define TDB_O_APPEND TD_FILE_APPEND
#define TDB_O_RDWR   (TD_FILE_WRITE) | (TD_FILE_READ)

#define tdbOsOpen(PATH, OPTION, MODE) taosOpenFile((PATH), (OPTION))
#define tdbOsClose(FD)                taosCloseFile(&(FD))
#define tdbOsRead                     taosReadFile
#define tdbOsPRead                    taosPReadFile
#define tdbOsWrite                    taosWriteFile
#define tdbOsPWrite                   taosPWriteFile
#define tdbOsFSync                    taosFsyncFile
#define tdbOsLSeek                    taosLSeekFile
#define tdbDirPtr                     TdDirPtr
#define tdbDirEntryPtr                TdDirEntryPtr
#define tdbReadDir                    taosReadDir
#define tdbGetDirEntryName            taosGetDirEntryName
#define tdbDirEntryBaseName           taosDirEntryBaseName
#define tdbCloseDir                   taosCloseDir
#define tdbOsRemove                   remove
#define tdbOsFileSize(FD, PSIZE)      taosFStatFile(FD, PSIZE, NULL)

/* directory */
#define tdbOsMkdir taosMkDir
#define tdbOsRmdir taosRemoveDir

// For threads and lock -----------------
/* spin lock */
typedef TdThreadSpinlock tdb_spinlock_t;

#define tdbSpinlockInit    taosThreadSpinInit
#define tdbSpinlockDestroy taosThreadSpinDestroy
#define tdbSpinlockLock    taosThreadSpinLock
#define tdbSpinlockUnlock  taosThreadSpinUnlock
#define tdbSpinlockTrylock taosThreadSpinTrylock

/* mutex lock */
typedef TdThreadMutex tdb_mutex_t;

#define tdbMutexInit    taosThreadMutexInit
#define tdbMutexDestroy taosThreadMutexDestroy
#define tdbMutexLock    taosThreadMutexLock
#define tdbMutexUnlock  taosThreadMutexUnlock

#else

// For memory -----------------
#define tdbOsMalloc  malloc
#define tdbOsCalloc  calloc
#define tdbOsRealloc realloc
#define tdbOsFree    free

// For file and directory -----------------
/* file */
typedef int tdb_fd_t;

#define TDB_O_CREAT  O_CREAT
#define TDB_O_WRITE  O_WRONLY
#define TDB_O_READ   O_RDONLY
#define TDB_O_TRUNC  O_TRUNC
#define TDB_O_APPEND O_APPEND
#define TDB_O_RDWR   O_RDWR

#define tdbOsOpen(PATH, OPTION, MODE) open((PATH), (OPTION), (MODE))

#define tdbOsClose(FD) \
  do {                 \
    close(FD);         \
    (FD) = -1;         \
  } while (0)

i64 tdbOsRead(tdb_fd_t fd, void *pData, i64 nBytes);
i64 tdbOsPRead(tdb_fd_t fd, void *pData, i64 nBytes, i64 offset);
i64 tdbOsWrite(tdb_fd_t fd, const void *pData, i64 nBytes);

#define tdbOsFSync  fsync
#define tdbOsLSeek  lseek
#define tdbOsRemove remove
#define tdbOsFileSize(FD, PSIZE)

/* directory */
#define tdbOsMkdir mkdir
#define tdbOsRmdir rmdir

// For threads and lock -----------------
/* spin lock */
typedef pthread_spinlock_t tdb_spinlock_t;

#define tdbSpinlockInit    pthread_spin_init
#define tdbSpinlockDestroy pthread_spin_destroy
#define tdbSpinlockLock    pthread_spin_lock
#define tdbSpinlockUnlock  pthread_spin_unlock
#define tdbSpinlockTrylock pthread_spin_trylock

/* mutex lock */
typedef pthread_mutex_t tdb_mutex_t;

#define tdbMutexInit    pthread_mutex_init
#define tdbMutexDestroy pthread_mutex_destroy
#define tdbMutexLock    pthread_mutex_lock
#define tdbMutexUnlock  pthread_mutex_unlock

#endif

#ifdef __cplusplus
}
#endif

#endif /*_TDB_OS_H_*/
