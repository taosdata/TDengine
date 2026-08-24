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

#ifndef _TD_DND_QNODE_INT_H_
#define _TD_DND_QNODE_INT_H_

#include "dmUtil.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SBackupTask SBackupTask;

typedef struct {
  char    relPath[TSDB_FILENAME_LEN];
  uint8_t digest[SHA256_DIGEST_SIZE];
} SBackupFileDigest;

typedef struct {
  int32_t dnodeId;
  char    checkpoint[TSDB_FILENAME_LEN];
  uint8_t digest[SHA256_DIGEST_SIZE];
} SBackupCheckpointDigest;

typedef struct {
  int32_t dnodeId;
  char    config[TSDB_FILENAME_LEN];
  uint8_t digest[SHA256_DIGEST_SIZE];
} SBackupSnodeConfigDigest;

typedef struct {
  int32_t vgId;
  int64_t snapshotIndex;
  int32_t numOfChunks;
  uint8_t digest[SHA256_DIGEST_SIZE];
} SBackupVnodeDigest;

typedef struct {
  uint64_t magic;
  int64_t  snapshotIndex;
} SBackupVnodeDataHeader;

#define BACKUP_VNODE_DATA_MAGIC UINT64_C(0x54444E5644415441)
#define DM_BACKUP_SM4_BLOCK_SIZE 16
// Each vnode backup is split into equal-sized chunk files (the last one may be
// smaller) so a single vnode never exceeds the max file size of the target
// filesystem. The chunk size MUST be a multiple of DM_BACKUP_SM4_BLOCK_SIZE so
// that encrypted chunks always break on SM4 block boundaries.
#define DM_BACKUP_VNODE_CHUNK_SIZE (INT64_C(128) * 1024 * 1024) // 128MB

typedef struct {
  TdFilePtr pFile;
  bool      encrypted;
  uint8_t   key[ENCRYPT_KEY_LEN];
  uint8_t   iv[DM_BACKUP_SM4_BLOCK_SIZE];
  uint8_t   buffer[DM_BACKUP_SM4_BLOCK_SIZE];
  int32_t   bufferLen;
  bool      finalized;
  sha256_ctx checksumCtx;
  uint8_t   checksum[SHA256_DIGEST_SIZE];
  int64_t   fileSize;
  // chunk mode: a logical file split into equal-sized chunk files named
  // <chunkBase>/00000001, 00000002, ... The IV (encrypted mode) is only stored
  // at the head of chunk 1 and the padding block only in the final chunk, so
  // the whole logical file remains one continuous ciphertext stream.
  bool      chunked;
  bool      finalChunk; // write mode: keep the padding block in the final chunk
  char      chunkDir[PATH_MAX];
  char      chunkBase[TSDB_FILENAME_LEN];
  int64_t   chunkSize;   // write: DM_BACKUP_VNODE_CHUNK_SIZE; read: size of chunk 1
  int32_t   chunkIndex;  // 1-based current chunk
  int32_t   numOfChunks; // read mode: total chunks; write mode: filled by close
  int64_t   chunkWritten;// write mode: bytes written to the current chunk file
} SDmBackupFile;

typedef struct {
  bool    enabled;
  bool    hasVerify;
  bool    verbose;
  bool    hasPassword;
  int8_t  verify;
  char    backupChainPath[PATH_MAX];
  char    password[TSDB_PASSWORD_LEN];
  int64_t lastBackupId;
  char    dbName[TSDB_DB_NAME_LEN];
} SDmRestoreOption;

typedef struct SDnodeMgmt {
  SDnodeData                  *pData;
  SMsgCb                       msgCb;
  STfs                        *pTfs;
  const char                  *path;
  const char                  *name;
  TdThread                     statusThread;
  TdThread                     configThread;
  TdThread                     keySyncThread;
  TdThread                     statusInfoThread;
  TdThread                     notifyThread;
  TdThread                     monitorThread;
  TdThread                     auditThread;
  TdThread                     crashReportThread;
  TdThread                     metricsThread;
  SSingleWorker                mgmtWorker;
  SSingleWorker                backupWorker;
  TdThreadMutex                backupMutex;
  SBackupTask                 *pBackupTask;
  ProcessCreateNodeFp          processCreateNodeFp;
  ProcessAlterNodeFp           processAlterNodeFp;
  ProcessAlterNodeTypeFp       processAlterNodeTypeFp;
  ProcessDropNodeFp            processDropNodeFp;
  SendMonitorReportFp          sendMonitorReportFp;
  SendMetricsReportFp          sendMetricsReportFp;
  MonitorCleanExpiredSamplesFp monitorCleanExpiredSamplesFp;
  MetricsCleanExpiredSamplesFp metricsCleanExpiredSamplesFp;
  SendAuditRecordsFp           sendAuditRecordsFp;
  GetVnodeLoadsFp              getVnodeLoadsFp;
  GetVnodeLoadsFp              getVnodeLoadsLiteFp;
  SetVnodeSyncTimeoutFp        setVnodeSyncTimeoutFp;
  GetMnodeLoadsFp              getMnodeLoadsFp;
  SetMnodeSyncTimeoutFp        setMnodeSyncTimeoutFp;
  GetQnodeLoadsFp              getQnodeLoadsFp;
  CollectVnodeTxnIdleFp        collectVnodeTxnIdleFp;
  int32_t                      statusSeq;
  SDispatchWorkerPool          streamMgmtWorker;
} SDnodeMgmt;

struct SBackupTask {
  SBackupDnodeReq req;
  int64_t         totalSize;
  int32_t         totalVgroups;
  int32_t         finishedVgroups;
  int32_t         currentVgId;
  int32_t         code;
  int64_t         endTime;
  uint8_t         mnodeChecksum[SHA256_DIGEST_SIZE];
  SArray         *snodeCheckpoints;  // SBackupCheckpointDigest
  SArray         *snodeConfigs;      // SBackupSnodeConfigDigest
  SArray         *vnodeSnapshots;    // SBackupVnodeDigest
  bool            running;
  bool            cancelled;
};

typedef struct {
  SBackupTask *pTask;
} SBackupTaskItem;

// dmHandle.c
SArray *dmGetMsgHandles();
void    dmSendStatusReq(SDnodeMgmt *pMgmt);
void    dmSendConfigReq(SDnodeMgmt *pMgmt);
void    dmSendKeySyncReq(SDnodeMgmt *pMgmt);
void    dmUpdateStatusInfo(SDnodeMgmt *pMgmt);
void    dmSendNotifyReq(SDnodeMgmt *pMgmt, SNotifyReq *pReq);
int32_t dmProcessConfigReq(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t dmProcessBackupReq(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);
void    dmBackupFreeTaskArtifacts(SBackupTask *pTask);
int32_t dmProcessBackupTask(SDnodeMgmt *pMgmt, SBackupTask *pTask);
int32_t dmProcessBackupProgressReq(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t dmProcessKillBackupReq(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t dmRestoreFromBackupChain(const SDmRestoreOption *pOpt);
int32_t dmProcessKeySyncRsp(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t dmProcessAuthRsp(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t dmProcessGrantRsp(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t dmProcessServerRunStatus(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t dmProcessRetrieve(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t dmProcessGrantReq(void *pInfo, SRpcMsg *pMsg);
int32_t dmProcessGrantNotify(void *pInfo, SRpcMsg *pMsg);
int32_t dmProcessCreateEncryptKeyReq(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t dmProcessAlterEncryptKeyReq(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t dmProcessAlterKeyExpirationReq(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t dmVerifyAndInitEncryptionKeys(void);

int32_t dmProcessReloadTlsConfig(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);

// dmWorker.c
int32_t dmPutNodeMsgToMgmtQueue(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t dmPutBackupTaskToQueue(SDnodeMgmt *pMgmt, SBackupTaskItem *pTaskItem);
int32_t dmStartStatusThread(SDnodeMgmt *pMgmt);
int32_t dmStartConfigThread(SDnodeMgmt *pMgmt);
int32_t dmStartKeySyncThread(SDnodeMgmt *pMgmt);
int32_t dmStartStatusInfoThread(SDnodeMgmt *pMgmt);
void    dmStopStatusThread(SDnodeMgmt *pMgmt);
void    dmStopConfigThread(SDnodeMgmt *pMgmt);
void    dmStopKeySyncThread(SDnodeMgmt *pMgmt);
void    dmStopStatusInfoThread(SDnodeMgmt *pMgmt);
int32_t dmStartNotifyThread(SDnodeMgmt *pMgmt);
void    dmStopNotifyThread(SDnodeMgmt *pMgmt);
int32_t dmStartMonitorThread(SDnodeMgmt *pMgmt);
int32_t dmStartAuditThread(SDnodeMgmt *pMgmt);
void    dmStopMonitorThread(SDnodeMgmt *pMgmt);
void    dmStopAuditThread(SDnodeMgmt *pMgmt);
int32_t dmStartCrashReportThread(SDnodeMgmt *pMgmt);
void    dmStopCrashReportThread(SDnodeMgmt *pMgmt);
int32_t dmStartMetricsThread(SDnodeMgmt *pMgmt);
void    dmStopMetricsThread(SDnodeMgmt *pMgmt);
int32_t dmStartWorker(SDnodeMgmt *pMgmt);
void    dmStopWorker(SDnodeMgmt *pMgmt);
int32_t dmPutMsgToStreamMgmtQueue(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);
int32_t dmProcessStreamHbRsp(SDnodeMgmt *pMgmt, SRpcMsg *pMsg);

#ifdef __cplusplus
}
#endif

#endif /*_TD_DND_QNODE_INT_H_*/
