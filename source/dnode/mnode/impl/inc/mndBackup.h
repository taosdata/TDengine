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

#ifndef _TD_MND_BACKUP_H_
#define _TD_MND_BACKUP_H_

#include "mndInt.h"

#ifdef __cplusplus
extern "C" {
#endif

int32_t mndInitBackup(SMnode *pMnode);
void    mndCleanupBackup(SMnode *pMnode);

SSdbRaw *mndBackupActionEncode(SBackupObj *pBackup);
SSdbRow *mndBackupActionDecode(SSdbRaw *pRaw);

int32_t mndBackupActionInsert(SSdb *pSdb, SBackupObj *pBackup);
int32_t mndBackupActionDelete(SSdb *pSdb, SBackupObj *pBackup);
int32_t mndBackupActionUpdate(SSdb *pSdb, SBackupObj *pOldBackup, SBackupObj *pNewBackup);

SBackupObj *mndAcquireBackup(SMnode *pMnode, int64_t backupId);
void        mndReleaseBackup(SMnode *pMnode, SBackupObj *pBackup);

int32_t mndRetrieveBackups(SRpcMsg *pReq, SShowObj *pShow, SSDataBlock *pBlock, int32_t rows);

int32_t mndProcessBackupReq(SRpcMsg *pReq);
int32_t mndProcessKillBackupReq(SRpcMsg *pReq);
void    mndBackupPullup(SMnode *pMnode);

#ifdef __cplusplus
}
#endif

#endif /*_TD_MND_BACKUP_H_*/
