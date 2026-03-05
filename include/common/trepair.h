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

#ifndef _TD_COMMON_REPAIR_H_
#define _TD_COMMON_REPAIR_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "os.h"
#include "taoserror.h"

typedef enum {
  REPAIR_NODE_TYPE_INVALID = 0,
  REPAIR_NODE_TYPE_VNODE = 1,
  REPAIR_NODE_TYPE_MNODE,
  REPAIR_NODE_TYPE_DNODE,
  REPAIR_NODE_TYPE_SNODE,
} ERepairNodeType;

typedef enum {
  REPAIR_FILE_TYPE_INVALID = 0,
  REPAIR_FILE_TYPE_WAL = 1,
  REPAIR_FILE_TYPE_TSDB,
  REPAIR_FILE_TYPE_META,
  REPAIR_FILE_TYPE_DATA,
  REPAIR_FILE_TYPE_CONFIG,
  REPAIR_FILE_TYPE_CHECKPOINT,
} ERepairFileType;

// Backward-compatible alias for previous name.
#define REPAIR_FILE_TYPE_TDB REPAIR_FILE_TYPE_META

typedef enum {
  REPAIR_MODE_INVALID = 0,
  REPAIR_MODE_FORCE = 1,
  REPAIR_MODE_REPLICA,
  REPAIR_MODE_COPY,
} ERepairMode;

typedef struct {
  bool            hasNodeType;
  ERepairNodeType nodeType;
  bool            hasFileType;
  ERepairFileType fileType;
  bool            hasVnodeIdList;
  char            vnodeIdList[PATH_MAX];
  bool            hasBackupPath;
  char            backupPath[PATH_MAX];
  bool            hasMode;
  ERepairMode     mode;
  bool            hasReplicaNode;
  char            replicaNode[PATH_MAX];
} SRepairCliArgs;

#define REPAIR_SESSION_ID_LEN 64
#define REPAIR_MAX_VNODE_IDS  128

typedef struct {
  bool            enabled;
  int64_t         startTimeMs;
  char            sessionId[REPAIR_SESSION_ID_LEN];
  ERepairNodeType nodeType;
  ERepairFileType fileType;
  ERepairMode     mode;
  bool            hasVnodeIdList;
  char            vnodeIdList[PATH_MAX];
  int32_t         vnodeIdNum;
  int32_t         vnodeIds[REPAIR_MAX_VNODE_IDS];
  bool            hasBackupPath;
  char            backupPath[PATH_MAX];
  bool            hasReplicaNode;
  char            replicaNode[PATH_MAX];
} SRepairCtx;

typedef struct {
  int32_t headFiles;
  int32_t dataFiles;
  int32_t smaFiles;
  int32_t sttFiles;
  int32_t unknownFiles;
} SRepairTsdbScanResult;

#define REPAIR_META_MAX_MISSING_FILES 16
#define REPAIR_META_FILE_NAME_LEN     64

typedef struct {
  int32_t requiredFiles;
  int32_t presentRequiredFiles;
  int32_t optionalIndexFiles;
  int32_t missingRequiredFiles;
  char    missingRequiredFileNames[REPAIR_META_MAX_MISSING_FILES][REPAIR_META_FILE_NAME_LEN];
} SRepairMetaScanResult;

typedef struct {
  int32_t walEvidenceFiles;
  int32_t tsdbRecoverableBlocks;
  int32_t inferredRules;
  bool    recoverable;
} SRepairMetaInferenceReport;

#define REPAIR_TSDB_MAX_REPORTED_BLOCKS 64

typedef struct {
  int32_t totalBlocks;
  int32_t recoverableBlocks;
  int32_t corruptedBlocks;
  int32_t unknownFiles;
  int32_t reportedCorruptedBlocks;
  char    corruptedBlockPaths[REPAIR_TSDB_MAX_REPORTED_BLOCKS][PATH_MAX];
} SRepairTsdbBlockReport;

typedef struct {
  bool    skipBackupPreparation;
  bool    resumeAtModeStep;
  int32_t backupStartVnodeIndex;
  int32_t replicaStartVnodeIndex;
  int32_t copyStartVnodeIndex;
  int32_t walStartVnodeIndex;
  int32_t tsdbStartVnodeIndex;
  int32_t metaStartVnodeIndex;
} SRepairResumePlan;

int32_t tRepairParseNodeType(const char *pNodeType, ERepairNodeType *pParsedNodeType);
int32_t tRepairParseFileType(const char *pFileType, ERepairFileType *pParsedFileType);
int32_t tRepairParseMode(const char *pMode, ERepairMode *pParsedMode);
int32_t tRepairExtractLongOptionValue(int32_t argc, char const *argv[], int32_t *pIndex, const char *optionName,
                                      const char **pOptionValue, bool *pMatched);
int32_t tRepairParseReplicaNodeEndpoint(const char *endpoint, char *host, int32_t hostSize, char *remoteDataDir,
                                        int32_t remoteDataDirSize);
int32_t tRepairParseCliOption(SRepairCliArgs *pCliArgs, const char *pOptionName, const char *pOptionValue);
int32_t tRepairValidateCliArgs(const SRepairCliArgs *pCliArgs);
int32_t tRepairInitCtx(const SRepairCliArgs *pCliArgs, int64_t startTimeMs, SRepairCtx *pCtx);
int32_t tRepairShouldRepairVnode(const SRepairCtx *pCtx, int32_t vnodeId, bool *pShouldRepair);
int32_t tRepairNeedRunWalForceRepair(const SRepairCtx *pCtx, bool *pNeedRun);
int32_t tRepairNeedRunTsdbForceRepair(const SRepairCtx *pCtx, bool *pNeedRun);
int32_t tRepairNeedRunMetaForceRepair(const SRepairCtx *pCtx, bool *pNeedRun);
int32_t tRepairNeedRunReplicaRepair(const SRepairCtx *pCtx, bool *pNeedRun);
int32_t tRepairNeedRunCopyRepair(const SRepairCtx *pCtx, bool *pNeedRun);
int32_t tRepairBuildCopySshProbeCmd(const char *replicaHost, const char *remoteTargetPath, char *cmd, int32_t cmdSize);
int32_t tRepairBuildCopyScpCmd(const char *replicaHost, const char *remoteTargetPath, const char *localTargetPath,
                               char *cmd, int32_t cmdSize);
int32_t tRepairDegradeReplicaVnode(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId, char *markerPath,
                                   int32_t markerPathSize);
int32_t tRepairRollbackReplicaVnode(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId);
int32_t tRepairWriteReplicaRestoreHint(const SRepairCtx *pCtx, const char *dataDir, char *hintPath,
                                       int32_t hintPathSize);
int32_t tRepairBuildVnodeTargetPath(const char *dataDir, int32_t vnodeId, ERepairFileType fileType,
                                    char *targetPath, int32_t targetPathSize);
int32_t tRepairScanTsdbFiles(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId,
                             SRepairTsdbScanResult *pResult);
int32_t tRepairScanMetaFiles(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId, SRepairMetaScanResult *pResult);
int32_t tRepairBuildMetaMissingFileMark(const SRepairMetaScanResult *pResult, char *mark, int32_t markSize);
int32_t tRepairInferMetaFromWalTsdb(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId,
                                    SRepairMetaInferenceReport *pReport);
int32_t tRepairRebuildMetaFiles(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId, const char *outputDir,
                                SRepairMetaScanResult *pResult);
int32_t tRepairAnalyzeTsdbBlocks(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId,
                                 SRepairTsdbBlockReport *pReport);
int32_t tRepairRebuildTsdbBlocks(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId, const char *outputDir,
                                 SRepairTsdbBlockReport *pReport);
int32_t tRepairPrecheck(const SRepairCtx *pCtx, const char *dataDir, int64_t minDiskAvailBytes);
int32_t tRepairPrepareBackupDir(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId, char *backupDir,
                                int32_t backupDirSize);
int32_t tRepairBackupVnodeTarget(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId, char *backupDir,
                                 int32_t backupDirSize);
int32_t tRepairRollbackVnodeTarget(const SRepairCtx *pCtx, const char *dataDir, int32_t vnodeId);
int32_t tRepairMockCopyReplicaVnodeTarget(const SRepairCtx *pCtx, const char *replicaDataDir, const char *localDataDir,
                                          int32_t vnodeId, char *srcPath, int32_t srcPathSize, char *dstPath,
                                          int32_t dstPathSize);
int32_t tRepairSshScpCopyReplicaVnodeTarget(const SRepairCtx *pCtx, const char *replicaHost,
                                            const char *replicaDataDir, const char *localDataDir, int32_t vnodeId,
                                            char *srcPath, int32_t srcPathSize, char *dstPath, int32_t dstPathSize);
int32_t tRepairPrepareSessionFiles(const SRepairCtx *pCtx, const char *dataDir, char *sessionDir,
                                   int32_t sessionDirSize, char *logPath, int32_t logPathSize, char *statePath,
                                   int32_t statePathSize);
int32_t tRepairAppendSessionLog(const char *logPath, const char *message);
int32_t tRepairWriteSessionState(const SRepairCtx *pCtx, const char *statePath, const char *step, const char *status,
                                 int32_t doneVnodes, int32_t totalVnodes);
int32_t tRepairTryResumeSession(SRepairCtx *pCtx, const char *dataDir, char *sessionDir, int32_t sessionDirSize,
                                char *logPath, int32_t logPathSize, char *statePath, int32_t statePathSize,
                                int32_t *pDoneVnodes, int32_t *pTotalVnodes, bool *pResumed, char *resumeStep,
                                int32_t resumeStepSize);
int32_t tRepairResolveResumePlan(ERepairNodeType nodeType, const char *resumeStep, int32_t doneVnodes,
                                 int32_t vnodeIdNum, SRepairResumePlan *pPlan);
int32_t tRepairNeedReportProgress(int64_t nowMs, int64_t intervalMs, int64_t *pLastReportMs, bool *pNeedReport);
int32_t tRepairBuildProgressLine(const SRepairCtx *pCtx, const char *step, int32_t doneVnodes, int32_t totalVnodes,
                                 char *line, int32_t lineSize);
int32_t tRepairBuildSummaryLine(const SRepairCtx *pCtx, int32_t successVnodes, int32_t failedVnodes, int64_t elapsedMs,
                                char *line, int32_t lineSize);

#ifdef __cplusplus
}
#endif

#endif /* _TD_COMMON_REPAIR_H_ */
