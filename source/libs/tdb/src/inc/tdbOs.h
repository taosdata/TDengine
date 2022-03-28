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

// TODO: kmake
#define TDB_FOR_TDENGINE

// For memor
#ifdef TDB_FOR_TDENGINE
#define tdbOsMalloc  taosMemoryMalloc
#define tdbOsCalloc  taosMemoryCalloc
#define tdbOsRealloc taosMemoryRealloc
#define tdbOsFree    taosMemoryFree
#else
#define tdbOsMalloc  malloc
#define tdbOsCalloc  calloc
#define tdbOsRealloc realloc
#define tdbOsFree    free
#endif

// For file
#ifdef TDB_FOR_TDENGINE
typedef TdFilePtr tdb_fd_t;

#define tdbOsOpen  taosOpenFile
#define tdbOsClose taosCloseFile
#define tdbOsRead  taosReadFile
#define tdbOsPRead taosPReadFile
#define tdbOsWrite taosWriteFile
#define tdbOsFSync taosFsyncFile
#else
#define tdbOsOpen  open
#define tdbOsClose close
#define tdbOsRead  read
#define tdbOsPRead pread
#define tdbOsWrite write
#define tdbOsFSync fsync
#endif

// For threads and lock
#ifdef TDB_FOR_TDENGINE

// spin lock
typedef TdThreadSpinlock tdb_spinlock_t;

#define tdbSpinlockInit    taosThreadSpinInit
#define tdbSpinlockDestroy taosThreadSpinDestroy
#define tdbSpinlockLock    taosThreadSpinLock
#define tdbSpinlockUnlock  taosThreadSpinUnlock
#define tdbSpinlockTrylock

// mutex lock
typedef TdThreadMutex tdb_mutex_t;

#define tdbMutexInit    taosThreadMutexInit
#define tdbMutexDestroy taosThreadMutexDestroy
#define tdbMutexLock    taosThreadMutexLock
#define tdbMutexUnlock  taosThreadMutexUnlock

#else

// spin lock
typedef pthread_spinlock_t tdb_spinlock_t;

#define tdbSpinlockInit    pthread_spin_init
#define tdbSpinlockDestroy pthread_spin_destroy
#define tdbSpinlockLock    pthread_spin_lock
#define tdbSpinlockUnlock  pthread_spin_unlock
#define tdbSpinlockTrylock pthread_spin_trylock

// mutex lock
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