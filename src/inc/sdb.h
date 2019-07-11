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

#ifndef TDENGINE_SDB_H
#define TDENGINE_SDB_H

#ifdef __cplusplus
extern "C" {
#endif

#include "taosmsg.h"
#include "tsdb.h"

extern int      sdbDebugFlag;
extern short    sdbPeerPort;
extern short    sdbSyncPort;
extern int      sdbMaxNodes;
extern int      sdbHbTimer;  // seconds
extern char     sdbZone[];
extern char     sdbMasterIp[];
extern char     sdbPrivateIp[];
extern char *   sdbStatusStr[];
extern char *   sdbRoleStr[];
extern void *   mnodeSdb;
extern int      sdbExtConns;
extern int      sdbMaster;
extern uint32_t sdbPublicIp;
extern uint32_t sdbMasterStartTime;
extern SIpList *pSdbIpList;
extern SIpList *pSdbPublicIpList;

extern void (*sdbWorkAsMasterCallback)();  // this function pointer will be set by taosd

enum _keytype {
  SDB_KEYTYPE_STRING, SDB_KEYTYPE_UINT32, SDB_KEYTYPE_AUTO, SDB_KEYTYPE_RECYCLE, SDB_KEYTYPE_MAX
};

#define SDB_ROLE_UNAPPROVED 0
#define SDB_ROLE_UNDECIDED  1
#define SDB_ROLE_MASTER     2
#define SDB_ROLE_SLAVE      3

#define SDB_STATUS_OFFLINE  0
#define SDB_STATUS_UNSYNCED 1
#define SDB_STATUS_SYNCING  2
#define SDB_STATUS_SERVING  3
#define SDB_STATUS_DELETED  4

enum _sdbaction {
  SDB_TYPE_INSERT,
  SDB_TYPE_DELETE,
  SDB_TYPE_UPDATE,
  SDB_TYPE_DECODE,
  SDB_TYPE_ENCODE,
  SDB_TYPE_BEFORE_BATCH_UPDATE,
  SDB_TYPE_BATCH_UPDATE,
  SDB_TYPE_AFTER_BATCH_UPDATE,
  SDB_TYPE_RESET,
  SDB_TYPE_DESTROY,
  SDB_MAX_ACTION_TYPES
};

void *sdbOpenTable(int maxRows, int32_t maxRowSize, char *name, char keyType, char *directory,
                   void *(*appTool)(char, void *, char *, int, int *));

void *sdbGetRow(void *handle, void *key);

int64_t sdbInsertRow(void *handle, void *row, int rowSize);

int sdbDeleteRow(void *handle, void *key);

int sdbUpdateRow(void *handle, void *row, int updateSize, char isUpdated);

void *sdbFetchRow(void *handle, void *pNode, void **ppRow);

int sdbBatchUpdateRow(void *handle, void *row, int rowSize);

int64_t sdbGetId(void *handle);

int64_t sdbGetNumOfRows(void *handle);

void sdbSaveSnapShot(void *handle);

void sdbCloseTable(void *handle);

int sdbRemovePeerByIp(uint32_t ip);

int sdbInitPeers(char *directory);

void sdbCleanUpPeers();

int sdbCfgNode(char *cont);

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_SDB_H
